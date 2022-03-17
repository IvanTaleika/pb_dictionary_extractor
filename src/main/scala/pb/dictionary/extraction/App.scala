package pb.dictionary.extraction

import grizzled.slf4j.Logger
import org.apache.spark.sql.SparkSession
import pb.dictionary.extraction.bronze.BronzeArea
import pb.dictionary.extraction.device.DeviceHighlightsDb
import pb.dictionary.extraction.golden.GoldenArea
import pb.dictionary.extraction.publish.{CsvPublishArea, GoogleSheetsArea, ManualEnrichmentArea}
import pb.dictionary.extraction.silver.{DictionaryApiDevWordDefiner, SilverArea}
import pb.dictionary.extraction.stage.StageArea

object App {
  private val logger = Logger(getClass)
  // APIs worth trying (partially based on https://medium.com/@martin.breuss/finding-a-useful-dictionary-api-52084a01503d)
  // Definition:
  // 1. https://dictionaryapi.dev/
  // 2. https://developer.oxforddictionaries.com/
  // 3. https://www.dictionaryapi.com/
  // Translation:
  // 1. https://cloud.google.com/translate
  // 2. https://docs.microsoft.com/en-us/azure/cognitive-services/translator/reference/v3-0-dictionary-examples
  // Usage Rating:
  // 1. https://books.google.com/ngrams

  // Flow: Retrieve from db file -> parse -> filter bookmarks -> cleanse -> merge into delta -> extract new
  // -> define -> checkout golden delta by normalized form and definition -> extract new -> translate -> merge into golden delta
  // -> extract as CSV (override???) -> send to google sheets ???
  // Q: will we be able to sort google sheet by the timestamp/book/usage index
  // Q: can we use https://books.google.com/ngrams/info for usage rating?
  private val GOOGLE_CREDENTIALS_FILE_PATH = "conf/credentials/google_service.json"

  protected[extraction] val SourceDbPath    = "D:/system/config/books.db"
  protected[extraction] val StageAreaPath   = "dictionary/stage"
  protected[extraction] val BronzeAreaPath  = "dictionary/bronze"
  protected[extraction] val SilverAreaPath  = "dictionary/silver"
  protected[extraction] val GoldenAreaPath  = "dictionary/golden"
  protected[extraction] val CsvPublishPath  = "dictionary/csvPublish"
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

  def updateDictionary(deviceHighlights: DeviceHighlightsDb,
                       stageArea: StageArea,
                       bronze: BronzeArea,
                       silverArea: SilverArea,
                       goldenArea: GoldenArea,
                       manualEnrichmentArea: ManualEnrichmentArea,
                       publisher: GoogleSheetsArea) = {
    // TODO: use meanegful names instead of upsert everywhere
    deviceHighlights.snapshot
      .transform(df => stageArea.upsert(df))
      .transform(df => bronze.upsert(df))
      .transform(df => silverArea.upsert(df))
      .transform(df => goldenArea.upsert(df))
      .transform(df => publisher.upsert(df))
      .transform { publishSnapshot =>
        val silverSnapshot = silverArea.snapshot
        manualEnrichmentArea.upsert(silverSnapshot, publishSnapshot)
      }

//    goldenArea.snapshot
//      .transform(df => publisher.upsert(df))
//      .transform { publishSnapshot =>
//        val silverSnapshot = silverArea.snapshot
//        manualEnrichmentArea.upsert(silverSnapshot, publishSnapshot)
//      }
  }
//  SparkSession.active.table("updateDictionary.silver").orderBy(org.apache.spark.sql.functions.col("occurrences").desc).show(false)
}
