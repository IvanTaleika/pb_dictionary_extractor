package pb.dictionary.extraction.silver
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpUriRequest}
import org.apache.http.HttpStatus
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}
import pb.dictionary.extraction.RemoteHttpEnrichmentException
import pb.dictionary.extraction.bronze.CleansedText
import pb.dictionary.extraction.enrichment.{ParallelRemoteHttpEnricher, RemoteHttpDfEnricher, RemoteHttpEnricher, RemoteHttpEnrichmentException}
import pb.dictionary.extraction.silver.DictionaryApiDevTextDefiner._

import java.net.URLEncoder
import javax.net.ssl.SSLHandshakeException
import scala.util.Try

/** A definition dictionary API powered by free [[https://dictionaryapi.dev/]] backend
  * (which itself claims to query [[https://en.wiktionary.org/]]).
  *
  * @param dfEnricher a DataFrame wrapper for [[DictionaryApiDevEnricher]]
  */
class DictionaryApiDevTextDefiner protected[silver](dfEnricher: DictionaryApiDevDfEnricher) extends TextDefinitionApi {
  private val rawDefinitionCol = "rawDefinition"

  def define(df: Dataset[CleansedText]): DataFrame = {
    val spark = SparkSession.active
    import spark.implicits._
    val rawDefinitionEncoder =
      RowEncoder.apply(df.schema.add(StructField(rawDefinitionCol, StringType, nullable = true)))
    val definedDf   = dfEnricher.enrich(df, rawDefinitionEncoder)
    val formattedDf = parseDefinitionJson(definedDf)
    formattedDf
  }

  /** Parses [[ResponseStructure]] JSON keeping only attributes defined in [[DefinedText]]. */
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

    val formattedDf = thirdLevelExplodedDf
      .withColumnRenamed(NormalizedDefinition.WORD, DefinedText.NORMALIZED_TEXT)
      .withColumn(
        DefinedText.EXAMPLES,
        when(col(Definition.EXAMPLE).isNotNull && col(Definition.DEFINITION).isNotNull, array(col(Definition.EXAMPLE)))
          .when(col(Definition.DEFINITION).isNotNull, lit(Array.empty[String]))
      ) // API provides a single example only
      .drop(Definition.EXAMPLE)
    formattedDf
  }

}

object DictionaryApiDevTextDefiner {
  type DictionaryApiDevDfEnricher = RemoteHttpDfEnricher[CleansedText, Row]
  val ApiEndpoint = "https://api.dictionaryapi.dev/api/v2/entries/en"

  // rate limit is 450 request per 5 minutes. However, during USA day hours API just fails to keep up with request
  val SafeSingleTaskRps: Double = 1.49

  def apply(maxConcurrentConnections: Int = 1,
            singleTaskRps: Option[Double] = Option(SafeSingleTaskRps)): DictionaryApiDevTextDefiner = {
    val enricher: Option[Double] => DictionaryApiDevEnricher with DictionaryApiDevParallelHttpEnricher =
      new DictionaryApiDevEnricher(_) with DictionaryApiDevParallelHttpEnricher {
        override protected val concurrentConnections = maxConcurrentConnections
      }
    new DictionaryApiDevTextDefiner(new DictionaryApiDevDfEnricher(enricher, singleTaskRps))
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

/** Enriches [[CleansedText]] with string column that contains [[ResponseStructure]] JSON. */
abstract class DictionaryApiDevEnricher(singleTaskRps: Option[Double] = Option(SafeSingleTaskRps))
    extends RemoteHttpEnricher[CleansedText, Row](singleTaskRps) {
  def enrich(cleansedText: CleansedText): Row = {
    Row.fromSeq(cleansedText.productIterator.toSeq :+ requestEnrichment(cleansedText))
  }

  override protected def buildRequest(record: CleansedText) = {
    val url     = s"$ApiEndpoint/${URLEncoder.encode(record.text, "UTF-8")}"
    val request = new HttpGet(url)
    request
  }

  override protected def processResponse(response: Try[CloseableHttpResponse])(request: HttpUriRequest, i: Int) = {
    import DictionaryApiDevEnricher._
    response
      .map { validResponse =>
        validResponse.getStatusLine.getStatusCode match {
          case HttpStatus.SC_OK => super.processResponse(response)(request, i)
          case HttpStatus.SC_TOO_MANY_REQUESTS =>
            logger.warn(s"API limit exceeded on request `${request}`.")
            pauseRequestsAndRetry(TOO_MANY_REQUESTS_PAUSE_TIME_MS)
          case HttpStatus.SC_NOT_FOUND =>
            // Note that API can return 404 for words with definitions under a heavy load.
            // This is a main reason for SilverArea to retry the definition query for undefined words
            logger.info(s"Could not enrich request ${request}. No definition found")
            Option("")
          case _ =>
            throwUnknownStatusCodeException(request, validResponse)
        }
      }
      .recover {
        // API server may fail under heavy load, mostly during day-evening time in the US. In this
        // case it arbitrary terminate the connection. Recovery time is unknown, but 5 minutes seems
        // to be enough in most cases
        case e: SSLHandshakeException =>
          logger.error(s"Request `${request}` failed with exception", e)
          if (i < MAX_RETRIES) {
            logger.warn(
              s"Pausing requests execution before retrying the request `${request}`. Current attempt was `$i` from `$MAX_RETRIES`")
            pauseRequestsAndRetry(TOO_MANY_REQUESTS_PAUSE_TIME_MS)
          } else {
            val errorMessage =
              s"Failed to execute request `${request}` in `$MAX_RETRIES` attempts. " +
                s"Aborting execution with the latest exception"
            logger.error(errorMessage, e)
            throw RemoteHttpEnrichmentException(errorMessage, e)
          }
      }
      .get
  }
}

object DictionaryApiDevEnricher {
  private val MAX_RETRIES = 3
  // Rate limit is calculating over the 5 minutes window
  private val TOO_MANY_REQUESTS_PAUSE_TIME_MS = 5 * 60 * 1000
}

trait DictionaryApiDevParallelHttpEnricher extends ParallelRemoteHttpEnricher[CleansedText, Row] {
  override protected def remoteHostConnectTimeout = 2 * 60 * 1000
  override protected def socketResponseTimeout    = 3 * 60 * 1000
  override protected def connectionManagerTimeout = 6 * 60 * 1000
}
