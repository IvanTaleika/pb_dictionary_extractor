package pb.dictionary.extraction

import com.google.api.services.drive.{Drive, DriveScopes}
import com.google.api.services.sheets.v4.{Sheets, SheetsScopes}
import org.apache.spark.sql.Encoders
import org.scalatest.tags.Slow
import pb.dictionary.extraction.bronze.BronzeArea
import pb.dictionary.extraction.device.{PocketBookMark, PocketBookMarksArea}
import pb.dictionary.extraction.golden._
import pb.dictionary.extraction.publish.sheets.{GoogleServicesFactory, SheetsManualEnrichmentArea, SheetsPublishArea}
import pb.dictionary.extraction.silver.{DictionaryApiDevTextDefiner, SilverArea}
import pb.dictionary.extraction.stage.StageArea
import pb.dictionary.extraction.utils.FileUtils

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

@Slow
class AppTest extends TestBase {

  val testDir         = "target/AppTest/updateDictionary"
  val testSpreadsheet = "English_dev/Vocabulary_dev"
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
      val stageAreaPath             = s"$testDir/stage"
      val bronzeAreaPath            = s"$testDir/bronze"
      val silverAreaPath            = s"$testDir/silver"
      val goldenAreaPath            = s"$testDir/golden"
      val manualEnrichmentSheetPath = s"$testSpreadsheet/Manual"
      val vocabularySheetPath       = s"$testSpreadsheet/Main"

      val GOOGLE_CREDENTIALS_FILE_PATH           = "conf/credentials/google_service.json"
      val AZURE_TRANSLATOR_CREDENTIALS_FILE_PATH = "conf/credentials/azure_translator.json"

      val googleServicesFactory = new GoogleServicesFactory(appName, GOOGLE_CREDENTIALS_FILE_PATH)
      val driveService          = googleServicesFactory.create[Drive](DriveScopes.DRIVE_METADATA_READONLY)
      val spreadsheetsService   = googleServicesFactory.create[Sheets](SheetsScopes.SPREADSHEETS)

      // TOOD: create factory for azure resources
      val azureDictionaryTranslation = AzureDictionaryLookup(FileUtils.readToString(AZURE_TRANSLATOR_CREDENTIALS_FILE_PATH))

      val sampleFile = this.getClass.getResource("deviceHighlightsSample.csv").getPath

      val deviceHighlights      = stub[PocketBookMarksArea]
      val deviceHighlightSchema = Encoders.product[PocketBookMark].schema
      val appRunTimestamp       = Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
      val timestampProvider     = () => appRunTimestamp

      val stageArea  = new StageArea(stageAreaPath, timestampProvider)
      val bronzeArea = new BronzeArea(bronzeAreaPath, timestampProvider)
      val silverArea = new SilverArea(silverAreaPath, DictionaryApiDevTextDefiner(), timestampProvider)
      val goldenArea =
        new GoldenArea(goldenAreaPath, azureDictionaryTranslation, NgramUsageStatistics(), timestampProvider)
      val manualEnrichmentArea =
        new SheetsManualEnrichmentArea(manualEnrichmentSheetPath, driveService, spreadsheetsService, timestampProvider)
      val publish = new SheetsPublishArea(vocabularySheetPath, driveService, spreadsheetsService, timestampProvider)

      val deviceHighlightsSample = spark.read
        .format("csv")
        .options(Map("enforceSchema" -> "false", "mode" -> "FAILFAST", "multiline" -> "true", "header" -> "true"))
        .schema(deviceHighlightSchema)
        .load(sampleFile)
        .orderBy(PocketBookMark.OID)
      val oidCountPerSample = 5
      (deviceHighlights.snapshot _).when().onCall { () =>
        val stageCount    = stageArea.snapshot.count()
        val deviceDbState = deviceHighlightsSample.limit(stageCount.toInt + oidCountPerSample)
        deviceDbState.as[PocketBookMark]
      }

      App.updateGoogleSheets(
        deviceHighlights,
        stageArea,
        bronzeArea,
        silverArea,
        goldenArea,
        manualEnrichmentArea,
        publish
      )
//      Thread.sleep(1000000)
    }
  }
}
