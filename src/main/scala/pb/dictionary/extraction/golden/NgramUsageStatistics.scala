package pb.dictionary.extraction.golden
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpUriRequest}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.HttpStatus
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import pb.dictionary.extraction.{ParallelRemoteHttpEnricher, RemoteHttpDfEnricher, RemoteHttpEnricher, RemoteHttpEnrichmentException}
import pb.dictionary.extraction.golden.NgramUsageStatistics._

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
  val ApiEndpoint       = "https://books.google.com/ngrams/json"
  val SafeSingleTaskRps = 0.49

  def apply(corpus: String                = "eng_2019",
            yearStart: Int                = 2015,
            yearEnd: Int                  = 2019,
            maxConcurrentConnections: Int = 1): NgramUsageStatistics = {
    val client: Option[Double] => NgramEnricher with NgramParallelHttpEnricher =
      new NgramEnricher(corpus, yearStart, yearEnd)(_) with NgramParallelHttpEnricher {
        override protected val concurrentConnections = maxConcurrentConnections
      }
    new NgramUsageStatistics(new NgramDfEnricher(client, Option(SafeSingleTaskRps)))
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
    "noun"              -> "NOUN",
    "verb"              -> "VERB",
    "adjective"         -> "ADJ",
    "adverb"            -> "ADV",
    "pronoun"           -> "PRON",
    "determiner"        -> "DET",
    "preposition"       -> "ADP",
    "number"            -> "NUM",
    "conjunction"       -> "CONJ",
    "particle"          -> "PRT",
    "infinitive marker" -> "PRT",
    "exclamation"       -> "PRT",
    // this parts of speech were unseen in the requests, but we keep them just in case
    "postposition" -> "ADP",
    "article"      -> "DET",
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

  override protected def processResponse(response: Try[CloseableHttpResponse])(request: HttpUriRequest,  i: Int) = {
    import NgramEnricher._
    val successfulResponse = response.get
    val statusLine         = successfulResponse.getStatusLine
    statusLine.getStatusCode match {
      case HttpStatus.SC_OK =>
        super.processResponse(response)(request, i)
      case SC_TOO_MANY_REQUESTS =>
        logger.warn(s"Ngram API limit exceeded on request `${request}`.")
        pauseRequests(TOO_MANY_REQUESTS_PAUSE_TIME_MS)
        None
      case _ =>
        val body = super.processResponse(response)(request, i).get
        throw RemoteHttpEnrichmentException(
          s"Received response with unexpected statusCode for the request `${request}`." +
            s" Response status line `$statusLine`, body `$body`")
    }
  }
}

object NgramEnricher {
  private val SC_TOO_MANY_REQUESTS = 429
  // ngram API allows 30 requests in 1 minutes. Pausing for 1 minute should reset the counter
  private val TOO_MANY_REQUESTS_PAUSE_TIME_MS = 60 * 1000
}

trait NgramParallelHttpEnricher extends ParallelRemoteHttpEnricher[Row, Row] {
  override protected def remoteHostConnectTimeout = 30 * 1000
  override protected def socketResponseTimeout    = 1 * 60 * 1000
  override protected def connectionManagerTimeout = 2 * 60 * 1000
}
