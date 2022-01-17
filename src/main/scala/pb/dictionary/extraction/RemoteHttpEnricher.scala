package pb.dictionary.extraction

import com.google.common.util.concurrent.RateLimiter
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.nio.charset.StandardCharsets

abstract class RemoteHttpEnricher[In, Out](maxRequestsPerSecond: Option[Double] = None) extends Serializable {
  private lazy val rateLimiter: Option[RateLimiter] = maxRequestsPerSecond.map(RateLimiter.create)

  def enrich(record: In): Out

  protected def successfulResponseStatuses: Set[Int] = Set(HttpStatus.SC_OK)
  protected def httpClient: CloseableHttpClient
  protected def buildRequest(record: In): HttpUriRequest

  protected def requestEnrichment(record: In): String = {
    val request = buildRequest(record)
    rateLimiter.foreach(_.acquire())
    try {
      val response = httpClient.execute(request)
      try {
        if (successfulResponseStatuses.contains(response.getStatusLine.getStatusCode)) {
          EntityUtils.toString(response.getEntity, StandardCharsets.UTF_8)
        } else {
          null
        }
      } finally {
        response.close()
      }
    } catch {
      case e: Exception =>
        // FIXME: api.dictionaryapi.dev was down after a relatively small number of requests (no proofs on the number though)
        //  we need to have some exception handling mechanism + request timeouts + throttle
        throw e
    }
  }
}

trait ParallelRemoteHttpEnricher[In, Out] extends RemoteHttpEnricher[In, Out] {
  // The value must be equal to `spark.task.cpus`, cause a new object will be created for each task.
  // Spark default value is 1
  protected def concurrentConnections: Int = 1

  override lazy val httpClient: CloseableHttpClient = {
    val connManager = new PoolingHttpClientConnectionManager
    connManager.setMaxTotal(concurrentConnections)
    // This class queries a single HTTP route only
    connManager.setDefaultMaxPerRoute(concurrentConnections)
    HttpClientBuilder.create().setConnectionManager(connManager).build()
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
