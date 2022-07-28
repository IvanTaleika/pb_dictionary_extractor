package pb.dictionary.extraction

import org.scalatest.tags.Slow
import pb.dictionary.extraction.enrichment.RemoteHttpEnricher

import java.util.concurrent._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** [[RemoteHttpEnricherTestBase]] subclasses are working with real HTTP remote servers and intended
  * to be executed and observed manually.
  */
@Slow
abstract class RemoteHttpEnricherTestBase extends TestBase {

  def assertApiRequestLimits[T1, T2](
      api: RemoteHttpEnricher[T1, T2],
      testRecord: T1,
      isValidResponse: T2 => Boolean,
      maxRunningTimeMillis: Long,
      concurrency: Int
  ) = {
    def validEnrichmentsReport(orderedExecPeriods: Seq[(Long, Long)]) = {
      val nSuccessfulRequests    = orderedExecPeriods.size
      val latestExecEndTime      = orderedExecPeriods.maxBy(_._2)._2
      val earliestExecStartTime  = orderedExecPeriods.head._1
      val elapsedTime            = latestExecEndTime - earliestExecStartTime
      val elapsedTimeSec         = elapsedTime / 1000d
      val execTimes              = orderedExecPeriods.map(r => r._2 - r._1)
      val execTimesSec           = execTimes.map(_ / 1000d)
      val avgResponseWaitTime    = elapsedTime.toDouble / nSuccessfulRequests
      val avgResponseWaitTimeSec = avgResponseWaitTime / 1000d
      s"""Executed:
         |  `${elapsedTime}` (`${elapsedTimeSec} s`) - elapsed time
         |  `${nSuccessfulRequests}` successful requests
         |  `${execTimes.mkString("[", ", ", "]")}` (`${execTimesSec.mkString("[", "s, ", "s]")}`) - request waiting times
         |  `${avgResponseWaitTime}` (`${avgResponseWaitTimeSec} s`) - average response time
         |""".stripMargin
    }
    case class EnrichmentInvocationResult(enrichment: Try[T2], startTime: Long, endTime: Long)

    def enrichRecord(): EnrichmentInvocationResult = {
      val startTime = System.currentTimeMillis()
      val enrichment = Try {
        val res = api.enrich(testRecord)
        if (!isValidResponse(res)) {
          throw new Exception(s"Received invalid response ${res}")
        }
        res
      }
      val endTime = System.currentTimeMillis()
      EnrichmentInvocationResult(enrichment, startTime, endTime)
    }

    val enricher: () => Future[EnrichmentInvocationResult] = concurrency match {
      case c if c < 1 => throw new IllegalArgumentException(s"Concurrency must be >= 1, found `${c}`")
      case 1 =>
        () =>
          {
            val future = new FutureTask(() => enrichRecord())
            future.run()
            future
          }
      case c if c > 1 =>
        val pool =
          new ThreadPoolExecutor(c - 1,
                                 c - 1,
                                 0L,
                                 TimeUnit.MILLISECONDS,
                                 new LinkedBlockingQueue[Runnable]((c - 1) * 2));
        pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy())
        () =>
          pool.submit(() => enrichRecord())
    }
    val futureEnrichments = ArrayBuffer.empty[Future[EnrichmentInvocationResult]]

    val executionStartTime = System.currentTimeMillis()
    val executionEndTime   = executionStartTime + maxRunningTimeMillis
    while (System.currentTimeMillis() < executionEndTime) {
      futureEnrichments.append(enricher())
    }
    val windowEnrichments = futureEnrichments.map(_.get()).filter(_.startTime < executionEndTime).sortBy(_.startTime)
    val validEnrichments  = windowEnrichments.takeWhile(_.enrichment.isSuccess).map(r => r.startTime -> r.endTime)
    val executionReport   = validEnrichmentsReport(validEnrichments)
    windowEnrichments.zipWithIndex.find(_._1.enrichment.isFailure).foreach {
      case (enrichmentResult, i) =>
        val failedRequestRunningTime    = enrichmentResult.endTime - enrichmentResult.startTime
        val failedRequestRunningTimeSec = failedRequestRunningTime / 1000d
        val errorMessage =
          s"""Execution failed on `${i}` element. Reason `${enrichmentResult.enrichment.failed.get.getMessage}`.
             |Failed request running time `${failedRequestRunningTime}` (`${failedRequestRunningTimeSec}`s)
             |$executionReport
             |""".stripMargin
        fail(errorMessage)
    }
    logger.info(executionReport)
  }

}
