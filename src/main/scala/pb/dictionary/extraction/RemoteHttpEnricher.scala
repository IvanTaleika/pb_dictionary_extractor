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

/** Transform [[In]] record into [[Out]] record by enriching [[In]] with data returned by an HTTP request.
  * This class is thread-safe, [[maxRequestsPerSecond]] are enforced per class instance, meaning max RPS
  * is the same no matter how many threads are using this class (however, fluctuations are possible).
  * In case a max RPS throttling is not enough (there can be accidental overuse of defined quotas), this
  * class provides utilities to block all the enrichment invocation for some grace period.
  *
  * @param maxRequestsPerSecond controls how many requests can be sent per second of execution,
  *                             by throttling records processing rate. This value specifies max RPS for a
  *                             infinite-time working enrichment loop. Real-time RPS can fluctuate around
  *                             this value.
  */
abstract class RemoteHttpEnricher[In, Out](protected val maxRequestsPerSecond: Option[Double] = None)
    extends Serializable {
  protected lazy val logger = Logger(getClass)

  // Locked when API limit is reached before the `requestsLock` to ensure only one thread is explicitly
  // sleeping the grace period.
  private lazy val requestPauseLock = new ReentrantLock()
  // Locked on read on before sending each HTTP request. Locked on write when API limit is reached and
  // all the requests must be stopped.
  private lazy val requestsLock = new ReentrantReadWriteLock()

  /** Requests throttler. */
  private lazy val rateLimiter: Option[RateLimiter] = maxRequestsPerSecond.map(RateLimiter.create)

  /** Transforms [[In]] record to [[Out]] record. Expects to call [[requestEnrichment]] method. */
  def enrich(record: In): Out

  protected def httpClient: CloseableHttpClient
  protected def buildRequest(record: In): HttpUriRequest

  /** returns a [[buildRequest]]-created HTTP request response. */
  protected def requestEnrichment(record: In): String = {
    val request    = buildRequest(record)
    var enrichment = Option.empty[String]
    var i          = 1
    do {
      if (!requestsLock.readLock().tryLock()) {
        logger.info(s"Waiting request execution to unlock for `${request}`.")
        requestsLock.readLock().lock()
      }
      val response = try {
        logger.info(
          s"Throttling request `${request}`. Request rate `${rateLimiter.map(r => s"${r.getRate} requests per second").getOrElse("unlimited")}`")
        rateLimiter.foreach(_.acquire())
        logger.info(s"Executing request `${request}`. Attempt `$i`.")
        Try(httpClient.execute(request))
      } finally {
        requestsLock.readLock().unlock()
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

  protected def consumeResponse(response: CloseableHttpResponse): String = {
    EntityUtils.toString(response.getEntity, StandardCharsets.UTF_8)
  }

  /** Handles the enrichment request response. The default implementation consumes and returns response body.
    *
    * @param response request response, or error.
    * @param request source request.
    * @param i a current request attempt, starting from 1.
    * @return [[Some]] if processing succeeded, [[None]] otherwise. If [[None]] is returned,
    *         [[requestEnrichment]] re-queues the request.
    */
  protected def processResponse(response: Try[CloseableHttpResponse])(request: HttpUriRequest,
                                                                      i: Int): Option[String] = {
    Option(consumeResponse(response.get))
  }

  protected def throwUnknownStatusCodeException(request: HttpUriRequest, response: CloseableHttpResponse): Nothing = {
    val statusLine = response.getStatusLine
    val body       = consumeResponse(response)
    throw RemoteHttpEnrichmentException(
      s"Received response with unexpected statusCode for the request `${request}`." +
        s" Response status line `$statusLine`, body `$body`")
  }

  /** Locks [[requestEnrichment]] methods invocation for [[pauseTimeMs]] period. */
  protected def pauseRequests(pauseTimeMs: Long): Unit = {
    // block other threads from entering sleep section.
    if (requestPauseLock.tryLock()) {
      // block other threads from sending the requests
      requestsLock.writeLock().lock()
      logger.warn(s"Pausing requests execution for ${pauseTimeMs} ms.")
      try {
        Thread.sleep(pauseTimeMs)
      } finally {
        requestsLock.writeLock().unlock()
        requestPauseLock.unlock()
      }
    } else {
      logger.warn(s"Requests are already paused by another thread.")
    }
  }

  protected def pauseRequestsAndRetry(pauseTimeMs: Long): Option[Nothing] = {
    pauseRequests(pauseTimeMs)
    None
  }

}

/** An mixin that plugs in a connection pool HTTP client to the Enricher.
  * The client initialization is lazy to be compatible with spark tasks execution model.
  */
trait ParallelRemoteHttpEnricher[In, Out] extends RemoteHttpEnricher[In, Out] {
  // Note that spark spawns a new enricher instance for each task. Each task operates by `spark.task.cpus`
  // cores (default value is 1). Making the actual parallelism = `concurrentConnections` * `spark tasks`
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

/** An [[RemoteHttpEnricher]] proxy to work with [[Dataset]]s.
  * The [[singleTaskRps]] requests RPS is further divided between enricher object,
  * initialized on the executors based on a number of concurrent tasks that will be spawned for the
  * [[Dataset]] processing
  */
class RemoteHttpDfEnricher[In, Out](enricherSummoner: Option[Double] => RemoteHttpEnricher[In, Out],
                                    singleTaskRps: Option[Double]) {

  def this(staticEnricher: RemoteHttpEnricher[In, Out]) = {
    this(_ => staticEnricher, None)
  }

  def enrich(ds: Dataset[In], outEncoder: Encoder[Out]): Dataset[Out] = {
    val adjusterRps =
      singleTaskRps.map(v => v / Math.max(ds.rdd.getNumPartitions, SparkSession.active.sparkContext.defaultParallelism))
    val enricher = enricherSummoner(adjusterRps)
    // cache the result to avoid repeating HTTP requests on Dataset recalculation
    ds.map(enricher.enrich, outEncoder).cache()
  }
}

case class RemoteHttpEnrichmentException(message: String, cause: Throwable = null)
    extends PbDictionaryException(message, cause)
