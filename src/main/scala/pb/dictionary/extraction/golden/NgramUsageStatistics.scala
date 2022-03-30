package pb.dictionary.extraction.golden
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpUriRequest}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.HttpStatus
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import pb.dictionary.extraction.{ParallelRemoteHttpEnricher, RemoteHttpDfEnricher, RemoteHttpEnricher}
import pb.dictionary.extraction.golden.NgramUsageStatistics._
import pb.dictionary.extraction.silver.DictionaryApiDevWordDefiner.SafeSingleTaskRps
import pb.dictionary.extraction.silver.PartOfSpeech

import scala.util.Try

class NgramUsageStatistics(dfEnricher: NgramDfEnricher) extends UsageFrequencyApi {
  private val ngramSearchColNames = Seq(DictionaryRecord.NORMALIZED_TEXT, DictionaryRecord.PART_OF_SPEECH)
  private val ngramSearchCols     = ngramSearchColNames.map(col)
  private val rawEnrichmentCol    = "rawDefinition"

  override def findUsageFrequency(df: DataFrame): DataFrame = {
    // There can be multiple meanings for the same text and part of speech pair. We can limit the number of requests by grouping
    val textDf = df.select(ngramSearchCols: _*).distinct()
    val enrichedTextEncoder =
      RowEncoder.apply(textDf.schema.add(StructField(rawEnrichmentCol, StringType, nullable = true)))
    val enrichedTextDf   = dfEnricher.enrich(textDf, enrichedTextEncoder)
    val textBooksUsageDf = parseUsageJson(enrichedTextDf)
    val sourceAlias      = "source"
    val enrichedAlias    = "enriched"
    val dataBooksUsageDf = df
      .as(sourceAlias)
      .join(textBooksUsageDf.as(enrichedAlias),
            ngramSearchColNames.map(cn => col(s"$sourceAlias.$cn") <=> col(s"$enrichedAlias.$cn")).reduce(_ && _))
      .select(col(s"$sourceAlias.*"), col(DictionaryRecord.USAGE))
    dataBooksUsageDf
  }

  private def parseUsageJson(df: DataFrame) = {
    import ResponseStructure._
    val parsedEnrichmentCol = "parsedEnrichment"

    val parsedEnrichmentDf = df.withColumn(
      parsedEnrichmentCol,
      from_json(col(rawEnrichmentCol), ArrayType(Encoders.product[NgramUsage].schema))
    )
    val zeroLevelExplodedDf = parsedEnrichmentDf
      .select((ngramSearchCols :+ explode_outer(col(parsedEnrichmentCol)).as(parsedEnrichmentCol)): _*)
    val firstLevelExplodedDf = zeroLevelExplodedDf
      .select(
        (
          ngramSearchCols :+
            explode_outer(col(s"$parsedEnrichmentCol.${NgramUsage.TIMESERIES}")).as(NgramUsage.TIMESERIES)
        ): _*)
    val textUsageDf = firstLevelExplodedDf
      .groupBy(ngramSearchCols: _*)
      .agg(avg(NgramUsage.TIMESERIES) as DictionaryRecord.USAGE)
    textUsageDf
  }
}

object NgramUsageStatistics {
  type NgramDfEnricher = RemoteHttpDfEnricher[Row, Row]
  val ApiEndpoint = "https://books.google.com/ngrams/json"
  // Ngram API allows 30 requests in 1 minutes, but using 0.5 sometimes triggers request 31 at the end of the minute
  val SafeSingleTaskRps = 0.5 - 0.02

  def apply(corpus: String                = "eng_2019",
            yearStart: Int                = 2015,
            yearEnd: Int                  = 2019,
            maxConcurrentConnections: Int = 1,
            singleTaskRps: Option[Double] = Option(SafeSingleTaskRps)): NgramUsageStatistics = {
    val client: Option[Double] => NgramEnricher with NgramParallelHttpEnricher =
      new NgramEnricher(corpus, yearStart, yearEnd)(_) with NgramParallelHttpEnricher {
        override protected val concurrentConnections = maxConcurrentConnections
      }
    new NgramUsageStatistics(new NgramDfEnricher(client, singleTaskRps))
  }

  object ResponseStructure {
    case class NgramUsage(
        ngram: String,
        parent: String,
        `type`: String,
        timeseries: Array[Double]
    )

    object NgramUsage {
      val NGRAM      = "ngram"
      val PARENT     = "parent"
      val TYPE       = "type"
      val TIMESERIES = "timeseries"
    }
  }
}

abstract class NgramEnricher(corpus: String, yearStart: Int, yearEnd: Int)(
    singleTaskRps: Option[Double] = Option(SafeSingleTaskRps)
) extends RemoteHttpEnricher[Row, Row](singleTaskRps) {
  private val partOfSpeechMapping = Map(
    PartOfSpeech.NOUN              -> "NOUN",
    PartOfSpeech.VERB              -> "VERB",
    PartOfSpeech.ADJECTIVE         -> "ADJ",
    PartOfSpeech.ADVERB            -> "ADV",
    PartOfSpeech.PRONOUN           -> "PRON",
    PartOfSpeech.DETERMINER        -> "DET",
    PartOfSpeech.PREPOSITION       -> "ADP",
    PartOfSpeech.NUMBER            -> "NUM",
    PartOfSpeech.CONJUNCTION       -> "CONJ",
    PartOfSpeech.PARTICLE          -> "PRT",
    PartOfSpeech.INFINITIVE_MARKER -> "PRT",
    PartOfSpeech.EXCLAMATION       -> "PRT",
    PartOfSpeech.POSTPOSITION      -> "ADP",
    PartOfSpeech.ARTICLE           -> "DET",
  )

  override def enrich(record: Row): Row = {
    Row.fromSeq(record.toSeq :+ requestEnrichment(record))
  }

  override protected def buildRequest(record: Row) = {
    val text            = record.getAs[String](DictionaryRecord.NORMALIZED_TEXT)
    val partOfSpeech    = record.getAs[String](DictionaryRecord.PART_OF_SPEECH)
    val partOfSpeechTag = partOfSpeechMapping.get(partOfSpeech).map(v => s"_${v}").getOrElse("")
    val uri = new URIBuilder(NgramUsageStatistics.ApiEndpoint)
      .addParameter("content", s"$text$partOfSpeechTag:$corpus")
      .addParameter("year_start", yearStart.toString)
      .addParameter("year_end", yearEnd.toString)
      .build()
    val request = new HttpGet(uri)
    request
  }

  override protected def processResponse(response: Try[CloseableHttpResponse])(request: HttpUriRequest, i: Int) = {
    import NgramEnricher._
    val validResponse = response.get
    val statusLine    = validResponse.getStatusLine
    statusLine.getStatusCode match {
      case HttpStatus.SC_OK =>
        super.processResponse(response)(request, i)
      case SC_TOO_MANY_REQUESTS =>
        pauseRequestsAndRetry(request, TOO_MANY_REQUESTS_PAUSE_TIME_MS)
      case _ =>
        throwUnknownStatusCodeException(request, validResponse)
    }
  }
}

object NgramEnricher {
  // ngram API allows 30 requests in 1 minutes. Pausing for 1 minute should reset the counter
  private val TOO_MANY_REQUESTS_PAUSE_TIME_MS = 60 * 1000
}

trait NgramParallelHttpEnricher extends ParallelRemoteHttpEnricher[Row, Row] {
  override protected def remoteHostConnectTimeout = 30 * 1000
  override protected def socketResponseTimeout    = 1 * 60 * 1000
  override protected def connectionManagerTimeout = 2 * 60 * 1000
}
