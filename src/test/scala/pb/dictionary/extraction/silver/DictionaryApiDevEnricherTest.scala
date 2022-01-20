package pb.dictionary.extraction.silver

import org.apache.spark.sql.Row
import org.scalatest.tags.Slow
import pb.dictionary.extraction.RemoteHttpEnricherTestBase
import pb.dictionary.extraction.bronze.CleansedText

@Slow
class DictionaryApiDevEnricherTest extends RemoteHttpEnricherTestBase {
  describe("enrich method") {
    val testRecord                = CleansedText("duck", null, -1, null, null, null)
    val validationTimeMs          = 6 * 60 * 1000
    def isValidResponse(res: Row) = res.getString(6) != null && res.getString(6).nonEmpty

    it("Should pause request execution without failing if application exceed the request limit") {
      val concurrency = 2
      val api = new DictionaryApiDevEnricher(Option(100)) with DictionaryApiDevParallelHttpEnricher {
        override protected val concurrentConnections = concurrency

      }
      assertApiRequestLimits(api, testRecord, isValidResponse, validationTimeMs, concurrency)
    }

    it("Should keep requests rate below the request limit with default configuration") {
      val api = new DictionaryApiDevEnricher() with DictionaryApiDevParallelHttpEnricher
      assertApiRequestLimits(api, testRecord, isValidResponse, validationTimeMs, 1)
    }

    it("Should handle data unknown to the API without errors") {
      val localTestRecord = CleansedText("lolkek", null, -1, null, null, null)
      def isLocalTestResponseValid(res: Row) = res.getString(6) != null
      val api = new DictionaryApiDevEnricher() with DictionaryApiDevParallelHttpEnricher
      assertApiRequestLimits(api, localTestRecord, isLocalTestResponseValid, validationTimeMs, 1)
    }
  }

}
