package pb.dictionary.extraction.golden

import org.apache.spark.sql.Row
import org.scalatest.tags.Slow
import pb.dictionary.extraction.RemoteHttpEnricherTestBase
import pb.dictionary.extraction.golden.NgramUsageStatistics.SafeSingleTaskRps

@Slow
class NgramEnricherTest extends RemoteHttpEnricherTestBase {
  var testRecord: Row = _

  val validationTimeMs          = 2 * 60 * 1000
  def isValidResponse(res: Row) = res.getString(2) != null && res.getString(2).nonEmpty

  override def beforeAll() = {
    super.beforeAll()
    testRecord = createDataFrame("normalizedText String, partOfSpeech String", Row("duck", "noun")).head
  }
  def defaultParamsEnricher(concurrency: Int, singleTaskRps: Option[Double]) = {
    new NgramEnricher("eng_2019", 2015, 2019)(singleTaskRps) with NgramParallelHttpEnricher {
      override protected val concurrentConnections = concurrency
    }
  }

  describe("enrich method") {

    it("Should pause request execution without failing if application exceed the request limit") {
      val api = defaultParamsEnricher(2, Option(10))
      assertApiRequestLimits(api, testRecord, isValidResponse, validationTimeMs, 2)
    }

    it("Should keep requests rate below the request limit with default configuration") {
      val api = defaultParamsEnricher(1,  Option(SafeSingleTaskRps))
      assertApiRequestLimits(api, testRecord, isValidResponse, validationTimeMs, 1)
    }

    it("Should handle data unknown to the API without errors") {
      val testRecord = createDataFrame("normalizedText String, partOfSpeech String", Row("lolkek", "noun")).head
      val api = defaultParamsEnricher(1,  Option(SafeSingleTaskRps))
      assertApiRequestLimits(api, testRecord, isValidResponse, 30 * 1000, 1)
    }
  }

}
