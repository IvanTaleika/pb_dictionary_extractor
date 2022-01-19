package pb.dictionary.extraction.silver
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpUriRequest}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}
import pb.dictionary.extraction.{ParallelRemoteHttpEnricher, RemoteHttpDfEnricher, RemoteHttpEnricher, RemoteHttpEnrichmentException}
import pb.dictionary.extraction.bronze.CleansedWord
import pb.dictionary.extraction.silver.DictionaryApiDevWordDefiner._

import java.net.URLEncoder
import java.time.{LocalTime, OffsetTime, ZoneId}
import scala.util.Try

class DictionaryApiDevWordDefiner protected[silver] (dfEnricher: DictionaryApiDevDfEnricher) extends WordDefinitionApi {
  private val rawDefinitionCol = "rawDefinition"

  def define(df: Dataset[CleansedWord]): Dataset[DefinedWord] = {
    val spark = SparkSession.active
    import spark.implicits._
    val rawDefinitionEncoder =
      RowEncoder.apply(df.schema.add(StructField(rawDefinitionCol, StringType, nullable = true)))
    val definedDf   = dfEnricher.enrich(df, rawDefinitionEncoder)
    val formattedDf = parseDefinitionJson(definedDf)
    formattedDf.as[DefinedWord]
  }

  private def parseDefinitionJson(df: DataFrame) = {
    import ResponseStructure._

    val sourceCols = df.schema.names.filterNot(_ == rawDefinitionCol).map(col)

    val lvl1Cols          = Seq(NormalizedDefinition.WORD, NormalizedDefinition.PHONETIC)
    val flatten1Cols      = sourceCols ++ lvl1Cols.map(col)
    val lvl2Cols          = Seq(Meaning.PART_OF_SPEECH)
    val flatten2Cols      = flatten1Cols ++ lvl2Cols.map(col)
    val lvl3Cols          = Seq(Definition.DEFINITION, Definition.EXAMPLE, Definition.SYNONYMS, Definition.ANTONYMS)
    val parsedResponseCol = "parsedResponse"

    val parsedDefinitionDf = df.withColumn(
      parsedResponseCol,
      from_json(
        col(rawDefinitionCol),
        ArrayType(Encoders.product[ResponseStructure.NormalizedDefinition].schema)
      )
    )
    val zeroLevelExplodedDf = parsedDefinitionDf
      .select((sourceCols :+ explode_outer(col(parsedResponseCol)).as(parsedResponseCol)): _*)

    val firstLevelExplodedDf = zeroLevelExplodedDf.select(
      (
        (sourceCols ++ lvl1Cols.map(n => col(s"$parsedResponseCol.$n").as(n))) :+
          explode_outer(col(s"$parsedResponseCol.${NormalizedDefinition.MEANINGS}")).as(parsedResponseCol)
      ): _*)

    val secondLevelExplodedDf = firstLevelExplodedDf
      .select(
        (
          (flatten1Cols ++ lvl2Cols.map(n => col(s"$parsedResponseCol.$n").as(n))) :+
            explode_outer(col(s"$parsedResponseCol.${Meaning.DEFINITIONS}")).as(parsedResponseCol)
        ): _*)

    val thirdLevelExplodedDf = secondLevelExplodedDf
      .select((flatten2Cols ++ lvl3Cols.map(n => col(s"$parsedResponseCol.$n").as(n))): _*)

    val formattedDf = thirdLevelExplodedDf.withColumnRenamed(NormalizedDefinition.WORD, DefinedWord.NORMALIZED_TEXT)
    formattedDf
  }

}

object DictionaryApiDevWordDefiner {
  type DictionaryApiDevDfEnricher = RemoteHttpDfEnricher[CleansedWord, Row]
  val ApiEndpoint = "https://api.dictionaryapi.dev/api/v2/entries/en"

  def SafeSingleTaskRps: Double = {
    // rate limit is 450 request per 5 minutes. However, during USA day hours API just fails to keep up with request
    if (OffsetTime.now(ZoneId.of("America/New_York")).toLocalTime.isAfter(LocalTime.of(8, 0))) {
      0.49
    } else {
      1.49
    }
  }

  def apply(maxConcurrentConnections: Int = 1): DictionaryApiDevWordDefiner = {
    val enricher: Option[Double] => DictionaryApiDevEnricher with DictionaryApiDevParallelHttpEnricher =
      new DictionaryApiDevEnricher(_) with DictionaryApiDevParallelHttpEnricher {
        override protected val concurrentConnections = maxConcurrentConnections
      }
    new DictionaryApiDevWordDefiner(new DictionaryApiDevDfEnricher(enricher, Option(SafeSingleTaskRps)))
  }

  object ResponseStructure {

    case class NormalizedDefinition(
        word: String,
        phonetic: String,
        meanings: Array[Meaning]
    )

    object NormalizedDefinition {
      val WORD     = "word"
      val PHONETIC = "phonetic"
      val MEANINGS = "meanings"
    }

    case class Meaning(
        partOfSpeech: String,
        definitions: Array[Definition]
    )

    object Meaning {
      val PART_OF_SPEECH = "partOfSpeech"
      val DEFINITIONS    = "definitions"
    }

    case class Definition(
        definition: String,
        example: String,
        synonyms: Array[String],
        antonyms: Array[String],
    )

    object Definition {
      val DEFINITION = "definition"
      val EXAMPLE    = "example"
      val SYNONYMS   = "synonyms"
      val ANTONYMS   = "antonyms"
    }
  }
}

abstract class DictionaryApiDevEnricher(singleTaskRps: Option[Double] = Option(SafeSingleTaskRps))
    extends RemoteHttpEnricher[CleansedWord, Row](singleTaskRps) {
  def enrich(cleansedWord: CleansedWord): Row = {
    Row.fromSeq(cleansedWord.productIterator.toSeq :+ requestEnrichment(cleansedWord))
  }

  override protected def buildRequest(record: CleansedWord) = {
    val url     = s"$ApiEndpoint/${URLEncoder.encode(record.text, "UTF-8")}"
    val request = new HttpGet(url)
    request
  }

  override protected def processResponse(response: Try[CloseableHttpResponse])(request: HttpUriRequest, i: Int) = {
    import DictionaryApiDevEnricher._
    response
      .map(_ => super.processResponse(response)(request, i))
      .recover {
        // FIXME: Specialize exception
        case e: Exception =>
          logger.error(s"Request `${request}` has failed with exception", e)
          if (i < MAX_ATTEMPTS) {
            logger.warn(
              s"Pausing requests execution before retrying the request `${request}`. Current attempt was `$i` from `$MAX_ATTEMPTS`")
            pauseRequests(TOO_MANY_REQUESTS_PAUSE_TIME_MS)
            None
          } else {
            val errorMessage =
              s"Failed to execute request `${request}` in `$MAX_ATTEMPTS` attempts. " +
                s"Aborting execution with the latest exception"
            logger.error(errorMessage, e)
            throw RemoteHttpEnrichmentException(errorMessage, e)
          }
      }
      .get
  }
}

object DictionaryApiDevEnricher {
  private val MAX_ATTEMPTS = 3
  // Rate limit is calculating over the 5 minutes window
  private val TOO_MANY_REQUESTS_PAUSE_TIME_MS = 5 * 60 * 1000
}

trait DictionaryApiDevParallelHttpEnricher extends ParallelRemoteHttpEnricher[CleansedWord, Row] {
  override protected def remoteHostConnectTimeout = 2 * 60 * 1000
  override protected def socketResponseTimeout    = 3 * 60 * 1000
  override protected def connectionManagerTimeout = 6 * 60 * 1000
}
