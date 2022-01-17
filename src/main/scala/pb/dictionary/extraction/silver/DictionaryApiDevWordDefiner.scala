package pb.dictionary.extraction.silver
import org.apache.http.client.methods.HttpGet
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}
import pb.dictionary.extraction.{ParallelRemoteHttpEnricher, RemoteHttpDfEnricher, RemoteHttpEnricher}
import pb.dictionary.extraction.bronze.CleansedWord
import pb.dictionary.extraction.silver.DictionaryApiDevWordDefiner._

import java.net.URLEncoder

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
  val ApiEndpoint       = "https://api.dictionaryapi.dev/api/v2/entries/en"
  val SafeSingleTaskRps = 0.5

  def apply(maxConcurrentConnections: Int = 1): DictionaryApiDevWordDefiner = {
    val enricher: Option[Double] => DictionaryApiDevEnricher with ParallelRemoteHttpEnricher[CleansedWord, Row] =
      new DictionaryApiDevEnricher(_) with ParallelRemoteHttpEnricher[CleansedWord, Row] {
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
}
