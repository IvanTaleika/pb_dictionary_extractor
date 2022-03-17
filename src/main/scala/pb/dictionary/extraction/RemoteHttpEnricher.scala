package pb.dictionary.extraction

import com.google.common.util.concurrent.RateLimiter
import grizzled.slf4j.Logger
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpUriRequest}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.nio.charset.StandardCharsets
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import scala.util.Try

abstract class RemoteHttpEnricher[In, Out](protected val maxRequestsPerSecond: Option[Double] = None)
    extends Serializable {
  protected val SC_TOO_MANY_REQUESTS = 429

  protected lazy val logger                         = Logger(getClass)
  private lazy val requestPauseLock                 = new ReentrantLock()
  private lazy val requestLock                      = new ReentrantReadWriteLock()
  private lazy val rateLimiter: Option[RateLimiter] = maxRequestsPerSecond.map(RateLimiter.create)

  def enrich(record: In): Out

  protected def httpClient: CloseableHttpClient
  protected def buildRequest(record: In): HttpUriRequest

  protected def requestEnrichment(record: In): String = {
    val request    = buildRequest(record)
    var enrichment = Option.empty[String]
    var i          = 1
    do {
      if (!requestLock.readLock().tryLock()) {
        logger.info(s"Waiting request execution to unlock for `${request}`.")
        requestLock.readLock().lock()
      }
      val response = try {
        logger.info(
          s"Throttling request `${request}`. Request rate `${rateLimiter.map(r => s"${r.getRate} requests per second").getOrElse("unlimited")}`")
        rateLimiter.foreach(_.acquire())
        logger.info(s"Executing request `${request}`. Attempt `$i`.")
        Try(httpClient.execute(request))
      } finally {
        requestLock.readLock().unlock()
      }
      try {
        enrichment = processResponse(response)(request, i)
      } finally {
        response.map(_.close())
      }
      i += 1
      if (enrichment.isEmpty)
        logger.info(s"Failed to receive enrichment for the request `${request}`. Awaiting next attempt `${i}`")
    } while (enrichment.isEmpty)
    val enrichmentString = enrichment.get
    logger.info(s"Received enrichment for the request `${request}`. Enrichment size `${enrichmentString.length}`")
    enrichmentString
  }

  protected def consumeResponse(response: CloseableHttpResponse) = {
    EntityUtils.toString(response.getEntity, StandardCharsets.UTF_8)
  }

  protected def processResponse(response: Try[CloseableHttpResponse])(request: HttpUriRequest, i: Int) = {
    Option(consumeResponse(response.get))
  }

  protected def throwUnknownStatusCodeException(request: HttpUriRequest, response: CloseableHttpResponse): Nothing = {
    val statusLine = response.getStatusLine
    val body       = consumeResponse(response)
    throw RemoteHttpEnrichmentException(
      s"Received response with unexpected statusCode for the request `${request}`." +
        s" Response status line `$statusLine`, body `$body`")
  }

  protected def pauseRequests(pauseTimeMs: Long): Unit = {
    if (requestPauseLock.tryLock()) {
      requestLock.writeLock().lock()
      logger.warn(s"Pausing requests execution for ${pauseTimeMs} ms.")
      try {
        Thread.sleep(pauseTimeMs)
      } finally {
        requestLock.writeLock().unlock()
        requestPauseLock.unlock()
      }
    } else {
      logger.warn(s"Requests are already paused by another thread.")
    }
  }

  protected def pauseRequestsAndRetry(request: HttpUriRequest, pauseTimeMs: Long) = {
    logger.warn(s"API limit exceeded on request `${request}`.")
    pauseRequests(pauseTimeMs)
    Option.empty[String]
  }

}

trait ParallelRemoteHttpEnricher[In, Out] extends RemoteHttpEnricher[In, Out] {
  // The value must be equal to `spark.task.cpus`, cause a new object will be created for each task.
  // Spark default value is 1
  protected def concurrentConnections: Int = 1

  protected def remoteHostConnectTimeout: Int
  protected def socketResponseTimeout: Int
  protected def connectionManagerTimeout: Int

  override lazy val httpClient: CloseableHttpClient = {
    val requestConfig = RequestConfig.custom
      .setConnectTimeout(remoteHostConnectTimeout)
      .setSocketTimeout(socketResponseTimeout)
      .setConnectionRequestTimeout(connectionManagerTimeout)
      .build
    val connManager = new PoolingHttpClientConnectionManager
    connManager.setMaxTotal(concurrentConnections)
    // This class queries a single HTTP route only
    connManager.setDefaultMaxPerRoute(concurrentConnections)
    HttpClientBuilder
      .create()
      .setConnectionManager(connManager)
      .setDefaultRequestConfig(requestConfig)
      .build()
  }
}

class RemoteHttpDfEnricher[In, Out](enricherSummoner: Option[Double] => RemoteHttpEnricher[In, Out],
                                    singleTaskRps: Option[Double]) {

  def this(staticEnricher: RemoteHttpEnricher[In, Out]) = {
    this(_ => staticEnricher, None)
  }

  def enrich(ds: Dataset[In], outEncoder: Encoder[Out]): Dataset[Out] = {
    val adjusterRps =
      singleTaskRps.map(v => v / Math.max(ds.rdd.getNumPartitions, SparkSession.active.sparkContext.defaultParallelism))
    val enricher = enricherSummoner(adjusterRps)
    ds.map(enricher.enrich, outEncoder).cache()
  }
}

case class RemoteHttpEnrichmentException(message: String, cause: Throwable = null)
    extends PbDictionaryException(message, cause)
