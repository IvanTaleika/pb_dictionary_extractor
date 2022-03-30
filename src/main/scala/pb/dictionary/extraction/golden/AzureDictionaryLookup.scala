package pb.dictionary.extraction.golden

import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost, HttpUriRequest}
import org.apache.http.HttpStatus
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}
import pb.dictionary.extraction.{
  ParallelRemoteHttpEnricher,
  RemoteHttpDfEnricher,
  RemoteHttpEnricher,
  RemoteHttpEnrichmentException
}
import pb.dictionary.extraction.silver.PartOfSpeech
import AzureDictionaryLookup._

import scala.util.Try

// https://docs.microsoft.com/en-us/azure/cognitive-services/translator/reference/v3-0-dictionary-lookup
class AzureDictionaryLookup protected[golden] (dfEnricher: DfEnricher) extends DictionaryTranslationApi {
  private val rawTranslationCol = "rawDefinition"

  // This Map does not required to be very precise
  private val partOfSpeechMapping = Map(
    "NOUN"  -> Seq(PartOfSpeech.NOUN, PartOfSpeech.NUMBER),
    "MODAL" -> Seq(PartOfSpeech.VERB),
    "ADJ"   -> Seq(PartOfSpeech.ADJECTIVE),
    "ADV"   -> Seq(PartOfSpeech.ADVERB),
    "PRON"  -> Seq(PartOfSpeech.PRONOUN),
    "CONJ"  -> Seq(PartOfSpeech.CONJUNCTION),
    "DET"   -> Seq(PartOfSpeech.DETERMINER, PartOfSpeech.ARTICLE),
    "PREP" -> Seq(PartOfSpeech.PREPOSITION,
                  PartOfSpeech.PARTICLE,
                  PartOfSpeech.INFINITIVE_MARKER,
                  PartOfSpeech.POSTPOSITION),
    "VERB" -> Seq(PartOfSpeech.VERB),
    "OTHER" -> Seq(PartOfSpeech.EXCLAMATION,
                   PartOfSpeech.PARTICLE,
                   PartOfSpeech.INFINITIVE_MARKER,
                   PartOfSpeech.POSTPOSITION)
  ).mapValues(_.mkString(","))

  def translate(goldenUpdates: DataFrame): DataFrame = {
    val spark = SparkSession.active
    import spark.implicits._
    val textDf = goldenUpdates.select(DictionaryRecord.NORMALIZED_TEXT).distinct().as[String]
    val rawTranslationEncoder =
      RowEncoder.apply(textDf.schema.add(StructField(rawTranslationCol, StringType, nullable = true)))
    val translatedText    = dfEnricher.enrich(textDf, rawTranslationEncoder)
    val translationParsed = parseDictionaryLookupJson(translatedText)
    val enrichedGolden    = enrichGolden(goldenUpdates, translationParsed)
    enrichedGolden
  }

  private def parseDictionaryLookupJson(df: DataFrame) = {
    import AzureDictionaryLookup.ResponseStructure._
    val sourceCols = df.schema.names.filterNot(_ == rawTranslationCol).map(col)

    val lvl1Cols          = Seq(DictionaryLookup.DISPLAY_SOURCE)
    val flatten1Cols      = sourceCols ++ lvl1Cols.map(col)
    val lvl2Cols          = Seq(Translation.DISPLAY_TARGET, Translation.POS_TAG, Translation.PREFIX_WORD)
    val parsedResponseCol = "parsedResponse"

    val parsedDefinitionDf = df.withColumn(
      parsedResponseCol,
      from_json(
        col(rawTranslationCol),
        ArrayType(Encoders.product[ResponseStructure.DictionaryLookup].schema)
      )
    )
    // TODO: Should in be just 1 to 1, cause we sent only 1 word?
    val zeroLevelExplodedDf = parsedDefinitionDf
      .select((sourceCols :+ explode_outer(col(parsedResponseCol)).as(parsedResponseCol)): _*)

    val firstLevelExplodedDf = zeroLevelExplodedDf.select(
      (
        (sourceCols ++ lvl1Cols.map(n => col(s"$parsedResponseCol.$n").as(n))) :+
          explode_outer(col(s"$parsedResponseCol.${DictionaryLookup.TRANSLATIONS}")).as(parsedResponseCol)
      ): _*)

    val secondLevelExplodedDf = firstLevelExplodedDf
      .select((flatten1Cols ++ lvl2Cols.map(n => col(s"$parsedResponseCol.$n").as(n))): _*)

    secondLevelExplodedDf
  }

  private def enrichGolden(goldenUpdates: DataFrame, parsedTranslations: DataFrame) = {
    import AzureDictionaryLookup.ResponseStructure._
    val formattedTranslationRnCol = "rowNumber"
    val translatedTextCol         = "translated_text"
    val noMatchesCol              = "noMatches"

    val formattedTranslations = parsedTranslations.na
      .replace(Translation.POS_TAG, partOfSpeechMapping)
      .withColumn(formattedTranslationRnCol, row_number().over(Window.orderBy(DictionaryRecord.NORMALIZED_TEXT)))
      .withColumnRenamed(DictionaryRecord.NORMALIZED_TEXT, translatedTextCol)
      .cache()

    val sourceAlias            = "source"
    val enrichedAlias          = "enriched"
    def colSource(n: String)   = col(s"$sourceAlias.$n")
    def colEnriched(n: String) = col(s"$enrichedAlias.$n")

    val partOfSpeechNoMatches =
      formattedTranslations
        .as(enrichedAlias)
        .join(
          goldenUpdates.as(sourceAlias),
          colSource(DictionaryRecord.NORMALIZED_TEXT) === colEnriched(translatedTextCol) &&
            colEnriched(Translation.POS_TAG).contains(colSource(DictionaryRecord.PART_OF_SPEECH)),
          "left_anti"
        )
        .select(col(formattedTranslationRnCol), lit(true) as noMatchesCol)

    val markedTranslations = formattedTranslations
      .join(partOfSpeechNoMatches, Seq(formattedTranslationRnCol), "left_outer")
      .withColumn(noMatchesCol, coalesce(col(noMatchesCol), lit(false)))

    val explodedGoldenUpdates = goldenUpdates
      .join(
        markedTranslations,
        col(DictionaryRecord.NORMALIZED_TEXT) === col(translatedTextCol) &&
          (col(noMatchesCol) || col(Translation.POS_TAG).contains(col(DictionaryRecord.PART_OF_SPEECH))),
        "left_outer"
      )

    val enrichedGoldenUpdates = explodedGoldenUpdates
    // In case there are no translations for some `PART_OF_SPEECH`, we stil want a `DISPLAY_SOURCE` instead of `null`
      .withColumn(
        DictionaryLookup.DISPLAY_SOURCE,
        first(col(DictionaryLookup.DISPLAY_SOURCE)) over (
          Window
            .partitionBy(DictionaryRecord.NORMALIZED_TEXT)
            .orderBy(col(DictionaryLookup.DISPLAY_SOURCE).asc_nulls_last)
            .rangeBetween(
              Window.unboundedPreceding,
              Window.unboundedFollowing
            )
          )
      )
      // Adding pretty-printed text form to possible forms with the same definition
      .withColumn(
        DictionaryLookup.DISPLAY_SOURCE,
        collect_set(DictionaryLookup.DISPLAY_SOURCE) over (Window.partitionBy(DictionaryRecord.pkCols: _*))
      )
      .withColumn(
        DictionaryRecord.FORMS,
        // Api promise that DictionaryLookup.DISPLAY_SOURCE won't be null
        array_union(col(DictionaryRecord.FORMS), col(DictionaryLookup.DISPLAY_SOURCE))
      )
      .withColumn(
        DictionaryRecord.FORMS,
        // Api promise that DictionaryLookup.DISPLAY_SOURCE won't be null
        array_union(col(DictionaryRecord.FORMS), col(DictionaryLookup.DISPLAY_SOURCE))
      )
      // Filling translation with possible prefixes
      .withColumn(
        Translation.DISPLAY_TARGET,
        when(
          col(Translation.PREFIX_WORD).isNotNull && col(Translation.PREFIX_WORD) =!= "",
          concat(col(Translation.DISPLAY_TARGET), lit(" ("), col(Translation.PREFIX_WORD), lit(")"))
        ).otherwise(col(Translation.DISPLAY_TARGET))
      )
      .withColumn(
        DictionaryRecord.TRANSLATIONS,
        // Collect_set does not store nulls
        collect_set(col(Translation.DISPLAY_TARGET)) over (Window.partitionBy(DictionaryRecord.pkCols: _*))
      )
      // dropping temporary translation columns
      .drop(markedTranslations.columns: _*)
      // dropping duplicates produced by multiple translations to a single word
      .dropDuplicates(DictionaryRecord.pk)
    enrichedGoldenUpdates
  }
}

object AzureDictionaryLookup {
  type DfEnricher = RemoteHttpDfEnricher[String, Row]
  val ApiEndpoint           = "https://api.cognitive.microsofttranslator.com/dictionary/lookup"
  val GlobalServiceLocation = "global"

  case class DictionaryLookupRequest(Text: String)
  // https://docs.microsoft.com/en-us/azure/cognitive-services/translator/reference/v3-0-reference#errors
  case class AzureErrorResponse(error: AzureError)
  case class AzureError(code: Long, message: String)

  def apply(serviceKey: String,
            fromLanguage: String          = "en",
            toLanguage: String            = "ru",
            serviceLocation: String       = GlobalServiceLocation,
            maxConcurrentConnections: Int = 1,
            singleTaskRps: Option[Double] = None): AzureDictionaryLookup = {
    val enricher: Option[Double] => AzureDictionaryLookupEnricher with AzureDictionaryLookupParallelHttpEnricher =
      new AzureDictionaryLookupEnricher(serviceKey, fromLanguage, toLanguage, serviceLocation)(_)
      with AzureDictionaryLookupParallelHttpEnricher {
        override protected val concurrentConnections = maxConcurrentConnections
      }
    new AzureDictionaryLookup(new DfEnricher(enricher, singleTaskRps))
  }

  object ResponseStructure {

    case class DictionaryLookup(
        normalizedSource: String,
        displaySource: String,
        translations: Array[Translation]
    )

    object DictionaryLookup {
      val NORMALIZED_SOURCE = "normalizedSource"
      val DISPLAY_SOURCE    = "displaySource"
      val TRANSLATIONS      = "translations"
    }

    case class Translation(
        normalizedTarget: String,
        displayTarget: String,
        posTag: String,
        confidence: Double,
        prefixWord: String,
        backTranslations: Array[BackTranslation]
    )

    object Translation {
      val NORMALIZED_TARGET = "normalizedTarget"
      val DISPLAY_TARGET    = "displayTarget"
      val POS_TAG           = "posTag"
      val CONFIDENCE        = "confidence"
      val PREFIX_WORD       = "prefixWord"
      val BACK_TRANSLATIONS = "backTranslations"
    }

    case class BackTranslation(
        normalizedText: String,
        displayText: String,
        numExamples: Int,
        frequencyCount: Int,
    )

    object Definition {
      val NORMALIZED_TEXT = "normalizedText"
      val DISPLAY_TEXT    = "displayText"
      val NUM_EXAMPLES    = "numExamples"
      val FREQUENCY_COUNT = "frequencyCount"
    }
  }
}

abstract class AzureDictionaryLookupEnricher(
    serviceKey: String,
    fromLanguage: String,
    toLanguage: String,
    serviceLocation: String = GlobalServiceLocation)(singleTaskRps: Option[Double] = None)
    extends RemoteHttpEnricher[String, Row](singleTaskRps) {
  import AzureDictionaryLookupEnricher._

  override def enrich(record: String): Row = {
    Row.fromTuple(record, requestEnrichment(record))
  }

  override protected def buildRequest(text: String) = {
    import io.circe.generic.auto._
    import io.circe.syntax._

    val uri = new URIBuilder(ApiEndpoint)
      .addParameter("api-version", s"3.0")
      .addParameter("from", fromLanguage)
      .addParameter("to", toLanguage)
      .build()
    val request = new HttpPost(uri)

    val body          = DictionaryLookupRequest(text)
    val requestEntity = new StringEntity(Seq(body).asJson.toString(), ContentType.APPLICATION_JSON)
    request.setEntity(requestEntity)

    request.addHeader("Ocp-Apim-Subscription-Key", serviceKey)
    request.addHeader("Ocp-Apim-Subscription-Region", serviceLocation)

    request
  }

  override protected def processResponse(response: Try[CloseableHttpResponse])(request: HttpUriRequest,
                                                                               i: Int): Option[String] = {
    val validResponse = response.get
    val statusLine    = validResponse.getStatusLine
    val body          = super.processResponse(response)(request, i)

    statusLine.getStatusCode match {
      case HttpStatus.SC_OK => body
      case _                => handleAzureServiceError(request, body, statusLine.getStatusCode, i)
    }
  }

  private def handleAzureServiceError(request: HttpUriRequest,
                                      responseBody: Option[String],
                                      responseCode: Int,
                                      i: Int): Option[String] = {
    import io.circe.generic.auto._
    import io.circe.parser.{decode => jsonDecode}
    val requestFailurePrefix = s"Request `${request}` execution has failed with code ${responseCode}"

    def handleBodyParseException(body: String, parseException: io.circe.Error): Nothing = {
      val azureDocNote =
        s"See https://docs.microsoft.com/en-us/azure/cognitive-services/translator/reference/v3-0-reference#errors for Azure API error details."
      logger.error(s"Failed to parse response body `$body` into Azure exception structure. $azureDocNote",
                   parseException)
      throw RemoteHttpEnrichmentException(
        s"$requestFailurePrefix and unknown response body structure `${body}`. $azureDocNote",
        parseException)
    }
    def handleAzureApiException(azureErrorResponse: AzureErrorResponse): Option[String] = {
      val azureException = azureErrorResponse.error
      val azureFailurePrefix =
        s"$requestFailurePrefix. Cause by Azure exception `${azureException.code}`, `${azureException.message}`."
      if ((azureException.code == UNEXPECTED_AZURE_ERROR_CODE || azureException.code == AZURE_SERVICE_UNAVAILABLE_CODE) && i <= MAX_RETRIES) {
        logger.warn(
          s"$azureFailurePrefix " +
            s"Pausing requests execution for `${AZURE_SERVICE_ERROR_RETRY_INTERVAL}` ms before retrying. " +
            s"Current attempt was `$i` from `$MAX_RETRIES`")
        pauseRequests(AZURE_SERVICE_ERROR_RETRY_INTERVAL)
        Option.empty[String]
      } else {
        val errorMessage =
          s"$azureFailurePrefix Aborting execution with the latest exception after `${i - 1}` retries."
        throw RemoteHttpEnrichmentException(errorMessage)
      }
    }

    responseBody
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(
        body =>
          jsonDecode[AzureErrorResponse](body).fold(
            handleBodyParseException(body, _),
            handleAzureApiException
        ))
      .getOrElse(throw RemoteHttpEnrichmentException(s"$requestFailurePrefix and empty body"))
  }
}

object AzureDictionaryLookupEnricher {
  val UNEXPECTED_AZURE_ERROR_CODE        = 500000L
  val AZURE_SERVICE_UNAVAILABLE_CODE     = 503000L
  val MAX_RETRIES                        = 3
  val AZURE_SERVICE_ERROR_RETRY_INTERVAL = 60 * 1000
}

trait AzureDictionaryLookupParallelHttpEnricher extends ParallelRemoteHttpEnricher[String, Row] {
  override protected def remoteHostConnectTimeout = 30 * 1000
  override protected def socketResponseTimeout    = 1 * 60 * 1000
  override protected def connectionManagerTimeout = 2 * 60 * 1000
}
