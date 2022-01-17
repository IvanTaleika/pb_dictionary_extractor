package pb.dictionary.extraction

import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.functions.lit
import pb.dictionary.extraction.bronze.{BronzeArea, CleansedWord}
import pb.dictionary.extraction.device.{DeviceHighlight, DeviceHighlights}
import pb.dictionary.extraction.golden._
import pb.dictionary.extraction.publish.GoogleSheets
import pb.dictionary.extraction.silver.{DictionaryApiDevEnricher, DictionaryApiDevWordDefiner, SilverArea}
import pb.dictionary.extraction.stage.StageArea

import scala.collection.mutable.ArrayBuffer

class AppTest extends TestBase {

  val testDir = "target/AppTest/updateDictionary"
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//    val dir = new Directory(new File(testDir))
//    if (dir.exists) {
//      dir.deleteRecursively()
//    }
//    dir.createDirectory()
//  }

  describe("updateDictionary") {
    it("should work") {
      val localSpark = spark
      import localSpark.implicits._
      val stageAreaPath           = s"$testDir/stage"
      val bronzeAreaPath          = s"$testDir/bronze"
      val silverAreaPath          = s"$testDir/silver"
      val goldenAreaPath          = s"$testDir/golden"
      val googleSheetsPublishPath = s"$testDir/publish"
      val sampleFile              = this.getClass.getResource("deviceHighlightsSample.csv").getPath

      val deviceHighlights      = stub[DeviceHighlights]
      val deviceHighlightSchema = Encoders.product[DeviceHighlight].schema
//      spark.executor.heartbeatInterval
//      spark.network.timeout
//      spark.scheduler.revive.interval
      val stageArea  = new StageArea(stageAreaPath)
      val bronzeArea = new BronzeArea(bronzeAreaPath)
      val silverArea = new SilverArea(silverAreaPath, DictionaryApiDevWordDefiner())
      val dummyUsageStatistics = new UsageFrequencyApi {
        override def findUsageFrequency(df: DataFrame): DataFrame = df.withColumn(DictionaryRecord.USAGE, lit(-1d))
      }
      val goldenArea = new GoldenArea(goldenAreaPath, new DummyDictionaryTranslator(), NgramUsageStatistics())
      val publish    = new GoogleSheets(googleSheetsPublishPath)
//
//      def deviceHighlightsSample =
//        spark.read.format("csv").options(CsvFilesOptions.ReadCsvOptions).schema(deviceHighlightSchema).load(sampleFile).sample(0.01)
//      var deviceDbState = spark.emptyDataset[DeviceHighlight].toDF()
//      (deviceHighlights.snapshot _).when().onCall { () =>
//
//        deviceDbState = deviceDbState.unionByName(deviceHighlightsSample).dropDuplicates(DeviceHighlight.OID)
//        deviceDbState.as[DeviceHighlight]
//      }

      val deviceHighlightsSample = spark.read
        .format("csv")
        .options(CsvFilesOptions.ReadCsvOptions)
        .schema(deviceHighlightSchema)
        .load(sampleFile)
        .orderBy(DeviceHighlight.OID)
      val oidCountPerSample = 100
      (deviceHighlights.snapshot _).when().onCall { () =>
        val stageCount    = stageArea.snapshot.count()
        val deviceDbState = deviceHighlightsSample.limit(stageCount.toInt + oidCountPerSample)
        deviceDbState.as[DeviceHighlight]
      }

      App.updateDictionary(deviceHighlights, stageArea, bronzeArea, silverArea, goldenArea, publish)
//      Thread.sleep(1000000)
    }

    it("Test ngrams max requests per second") {
      val api        = new NgramEnricher("eng_2019", 2015, 2019)() with ParallelRemoteHttpEnricher[Row, Row]
      val testRecord = createDataFrame("normalizedText String, partOfSpeech String", Row("duck", "noun")).head
      testApiRequestLimit(api, testRecord, (res: Row) => res.getString(2) == null, 2 * 60 * 1000)
    }

    it("Test Dictionary Api Dev max requests per second") {
      val api        = new DictionaryApiDevEnricher() with ParallelRemoteHttpEnricher[CleansedWord, Row]
      val testRecord = CleansedWord("duck", null, -1, null, null, null)
      testApiRequestLimit(api, testRecord, (res: Row) => res.getString(6) == null, 2 * 60 * 1000)
    }

    def testApiRequestLimit[T1, T2](
        api: RemoteHttpEnricher[T1, T2],
        record: T1,
        isValidResponse: T2 => Boolean,
        maxRunningTimeMillis: Long
    ) = {
      val execTimes = ArrayBuffer.empty[Long]
      def executionReport(reason: String) = {
        val elapsedTime            = execTimes.last - execTimes.head
        val elapsedTimeSec         = elapsedTime / 1000d
        val nSuccessfulRequests    = execTimes.size - 1
        val responseWaitTimes      = execTimes.sliding(2).map(buff => buff.last - buff.head).toSeq
        val responseWaitTimesSec   = responseWaitTimes.map(_ / 1000d)
        val avgResponseWaitTime    = responseWaitTimes.sum.toDouble / nSuccessfulRequests
        val avgResponseWaitTimeSec = avgResponseWaitTime / 1000d
        s"""Execution finished. Reason `${reason}`.
           |Executed:
           |  `${nSuccessfulRequests}` successful requests
           |  `${elapsedTime}` (`${elapsedTimeSec} s`) - elapsed time
           |  `${responseWaitTimes.mkString("[", ", ", "]")}` (`${responseWaitTimesSec
             .mkString("[", "s, ", "]")}`) - request waiting times
           |  `${avgResponseWaitTime}` (`${avgResponseWaitTimeSec} s`) - average response time
           |""".stripMargin
      }

      do {
        execTimes.append(System.currentTimeMillis())
        try {
          val res = api.enrich(record)
          if (isValidResponse(res)) {
            throw new Exception("empty response")
          }
        } catch {
          case e: Exception =>
            fail(executionReport(e.getMessage))
        }
      } while (execTimes.last - execTimes.head < maxRunningTimeMillis)
      println(executionReport("timeout"))
    }

//    it("should work with real db") {
//      val localSpark = spark
//      import localSpark.implicits._
//      val deviceHighlightsPath           = App.SourceDbPath
//      val stageAreaPath           = s"$testDir/stage"
//      val silverAreaPath          = s"$testDir/silver"
//      val goldenAreaPath          = s"$testDir/golden"
//      val googleSheetsPublishPath = s"$testDir/publish"
//
//      val deviceHighlights  = new DeviceHighlights(deviceHighlightsPath)
//      val stageArea  = new StageArea(stageAreaPath)
//      val silverArea = new SilverArea(silverAreaPath)
//      val goldenArea = new GoldenArea(goldenAreaPath)
//      val publish    = new GoogleSheets(googleSheetsPublishPath)
//
//      App.updateDictionary(deviceHighlights, stageArea, silverArea, goldenArea, publish)
//    }
  }
}
