package pb.dictionary.extraction

import grizzled.slf4j.Logger
import org.apache.spark.sql.SparkSession
import pb.dictionary.extraction.bronze.BronzeArea
import pb.dictionary.extraction.device.PocketBookMarksArea
import pb.dictionary.extraction.golden.GoldenArea
import pb.dictionary.extraction.publish.sheets.{SheetsManualEnrichmentArea, SheetsPublishArea}
import pb.dictionary.extraction.silver.SilverArea
import pb.dictionary.extraction.stage.StageArea

// TODO: transform to web app on Google? on Azure? with managed identity usage
object App {
  private val logger = Logger(getClass)

  private val GOOGLE_CREDENTIALS_FILE_PATH = "conf/credentials/google_service.json"

  protected[extraction] val SourceDbPath   = "D:/system/config/books.db"
  protected[extraction] val StageAreaPath  = "dictionary/stage"
  protected[extraction] val BronzeAreaPath = "dictionary/bronze"
  protected[extraction] val SilverAreaPath = "dictionary/silver"
  protected[extraction] val GoldenAreaPath = "dictionary/golden"
  protected[extraction] val CsvPublishPath = "dictionary/csvPublish"
//  protected[extraction] val GoogleSheetPath = "English_dev/Vocabulary_dev/Main"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("pb_dictionary_extractor")
      .getOrCreate()

//    val deviceHighlights = new DeviceHighlightsDb(SourceDbPath)
//    val stageArea        = new StageArea(StageAreaPath)
//    val bronzeArea       = new BronzeArea(BronzeAreaPath)
//    val silverArea       = new SilverArea(SilverAreaPath, DictionaryApiDevWordDefiner())
//    val goldenArea       = new GoldenArea(GoldenAreaPath)
//    val googleSheets     = new GoogleSheets(GoogleSheetsPublishPath)

//    updateDictionary(deviceHighlights, stageArea, bronzeArea, silverArea, goldenArea, googleSheets)
  }

  def updateGoogleSheets(deviceHighlights: PocketBookMarksArea,
                         stageArea: StageArea,
                         bronze: BronzeArea,
                         silverArea: SilverArea,
                         goldenArea: GoldenArea,
                         sheetsManualEnrichmentArea: SheetsManualEnrichmentArea,
                         sheetsPublisher: SheetsPublishArea) = {
    updateInternalDictionary(deviceHighlights, stageArea, bronze, silverArea, goldenArea)
      .transform(df => sheetsPublisher.merge(df))
      .transform { publishSnapshot =>
        val silverSnapshot = silverArea.snapshot
        sheetsManualEnrichmentArea.rewrite(silverSnapshot, publishSnapshot)
      }

//    goldenArea.snapshot
//      .transform(df => publisher.upsert(df.limit(10)))
//      .transform { publishSnapshot =>
//        val silverSnapshot = silverArea.snapshot
//        manualEnrichmentArea.upsert(silverSnapshot, publishSnapshot)
//      }
  }

  def updateInternalDictionary(deviceHighlights: PocketBookMarksArea,
                               stageArea: StageArea,
                               bronze: BronzeArea,
                               silverArea: SilverArea,
                               goldenArea: GoldenArea) = {
    deviceHighlights.snapshot
      .transform(df => stageArea.upsert(df))
      .transform(df => bronze.upsert(df))
      .transform(df => silverArea.upsert(df))
      .transform(df => goldenArea.upsert(df))
  }

//  SparkSession.active.table("updateDictionary.silver").orderBy(org.apache.spark.sql.functions.col("occurrences").desc).show(false)
}
