package pb.dictionary.extraction

import com.google.common.util.concurrent.RateLimiter
import grizzled.slf4j.Logger
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.functions.lit
import org.scalatest.tags.Slow
import pb.dictionary.extraction.bronze.{BronzeArea, CleansedText}
import pb.dictionary.extraction.device.{DeviceHighlight, DeviceHighlightsDb}
import pb.dictionary.extraction.golden._
import pb.dictionary.extraction.publish.{CsvPublishArea, GoogleSheetsArea, ManualEnrichmentArea}
import pb.dictionary.extraction.silver.{DictionaryApiDevEnricher, DictionaryApiDevParallelHttpEnricher, DictionaryApiDevWordDefiner, SilverArea}
import pb.dictionary.extraction.stage.StageArea

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}
import scala.collection.mutable.ArrayBuffer

@Slow
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
      val manualEnrichmentPath    = s"$testDir/manual_enrichment"
      val googleSheetsPublishPath = s"$testDir/publish"
      val sampleFile              = this.getClass.getResource("deviceHighlightsSample.csv").getPath

      val deviceHighlights      = stub[DeviceHighlightsDb]
      val deviceHighlightSchema = Encoders.product[DeviceHighlight].schema
      val appRunTimestamp       = Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
      val timestampProvider     = () => appRunTimestamp

      val stageArea  = new StageArea(stageAreaPath, timestampProvider)
      val bronzeArea = new BronzeArea(bronzeAreaPath, timestampProvider)
      val silverArea = new SilverArea(silverAreaPath, DictionaryApiDevWordDefiner(), timestampProvider)
      val goldenArea =
        new GoldenArea(goldenAreaPath, new DummyDictionaryTranslator(), NgramUsageStatistics(), timestampProvider)
      val manualEnrichmentArea = new ManualEnrichmentArea(manualEnrichmentPath, timestampProvider)
      val publish              = new CsvPublishArea(googleSheetsPublishPath, timestampProvider)

      val deviceHighlightsSample = spark.read
        .format("csv")
        .options(Map("enforceSchema" -> "false", "mode" -> "FAILFAST", "multiline" -> "true", "header" -> "true"))
        .schema(deviceHighlightSchema)
        .load(sampleFile)
        .orderBy(DeviceHighlight.OID)
      val oidCountPerSample = 100
      (deviceHighlights.snapshot _).when().onCall { () =>
        val stageCount    = stageArea.snapshot.count()
        val deviceDbState = deviceHighlightsSample.limit(stageCount.toInt + oidCountPerSample)
        deviceDbState.as[DeviceHighlight]
      }

      App.updateDictionary(
        deviceHighlights,
        stageArea,
        bronzeArea,
        silverArea,
        goldenArea,
        manualEnrichmentArea,
        publish
      )
      Thread.sleep(1000000)
    }
  }
}
