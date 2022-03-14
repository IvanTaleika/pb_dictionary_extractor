package pb.dictionary.extraction

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.drive.{Drive, DriveScopes}
import com.google.api.services.sheets.v4.{Sheets, SheetsScopes}
import com.google.api.services.sheets.v4.model.{
  BatchUpdateSpreadsheetRequest,
  BatchUpdateValuesRequest,
  DeleteSheetRequest,
  Request,
  SheetProperties,
  UpdateSheetPropertiesRequest,
  ValueRange
}
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import pb.dictionary.extraction.publish.SheetRow

import java.io.{File, FileInputStream, FileNotFoundException}
import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

object GoogleQuickstart {
  private val APPLICATION_NAME                 = "Sheets + Drive quickstart "
  private val JSON_FACTORY                     = GsonFactory.getDefaultInstance
  private val HTTP_TRANSPORT: NetHttpTransport = GoogleNetHttpTransport.newTrustedTransport

  /** Directory to store authorization tokens for this application. */
  // TODO: scopes
  private val SCOPES                = Seq(DriveScopes.DRIVE_METADATA_READONLY, SheetsScopes.SPREADSHEETS).asJava
  private val CREDENTIALS_FILE_PATH = "conf/credentials/google_service.json"
  private val TOKENS_DIRECTORY_PATH = "conf/tokens"

  private val nSheetIndexes            = 'Z' - 'A' + 1
  private val nVocabularyBackupsToKeep = 5

  private def getCredentials() = { // Load client secrets.
    val credentialsFiles = new File(CREDENTIALS_FILE_PATH)
    if (!credentialsFiles.exists()) {
      throw new FileNotFoundException(s"Credentials file not found: ${CREDENTIALS_FILE_PATH}")
    }
    val credentialsDescription = new FileInputStream(credentialsFiles)
    try {
      val credentials = GoogleCredentials.fromStream(credentialsDescription).createScoped(SCOPES)
      new HttpCredentialsAdapter(credentials)
    } finally {
      credentialsDescription.close()
    }
  }

  // TODO: include sheet clone (backup) and data write in the same batchUpdate that renames and delete sheets
  //   https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request
  def main(args: Array[String]): Unit = {
    val applicationStartTime = ZonedDateTime.now(ZoneOffset.UTC).toLocalDateTime

    val folderName      = "English_dev"
    val spreadsheetName = "Vocabulary_dev"
    val sheetName       = "Main"

    val driveService =
      new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials()).setApplicationName(APPLICATION_NAME).build
    val spreadsheetsService =
      new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials()).setApplicationName(APPLICATION_NAME).build

    def listRequest = driveService.files().list().setSpaces("drive")

    val vocabularyFolder = listRequest
      .setQ(s"mimeType = 'application/vnd.google-apps.folder' and name = '$folderName'")
      .setFields("files(id)")
      .execute()
      .getFiles
      .asScala
      .head

    val vocabularyFolderId = vocabularyFolder.getId
    println(s"Searching folder $vocabularyFolderId")

    val dictionarySpreadsheet = listRequest
      .setQ(s"mimeType='application/vnd.google-apps.spreadsheet' and '${vocabularyFolderId}' in parents")
      .setFields("files(id)")
      .execute()
      .getFiles
      .asScala
      .head

    val vocabularySpreadsheetId = dictionarySpreadsheet.getId

    println("Querying vocabulary spreadsheet sheets")

    val spreadsheetInfoRequest =
      spreadsheetsService.spreadsheets().get(vocabularySpreadsheetId).setIncludeGridData(false)
    val startingSpreadsheetInfo = spreadsheetInfoRequest.execute()
    val startingSheetsInfo      = startingSpreadsheetInfo.getSheets.asScala.map(_.getProperties)
    val mainSheetId             = startingSheetsInfo.find(_.getTitle == "Main").map(_.getSheetId).get

    println("Cloning the main sheet to a new one")

    import com.google.api.services.sheets.v4.model.CopySheetToAnotherSpreadsheetRequest
    val copySheetBody = new CopySheetToAnotherSpreadsheetRequest
    copySheetBody.setDestinationSpreadsheetId(vocabularySpreadsheetId)

    val copySheetRequest =
      spreadsheetsService.spreadsheets.sheets.copyTo(vocabularySpreadsheetId, mainSheetId, copySheetBody)
    val copySheetResult = copySheetRequest.execute()
    val newSheetId      = copySheetResult.getSheetId

    println(
      s"Renaming copied sheet and Clearing vocabulary spreadsheet to keep at most ${nVocabularyBackupsToKeep} backups")

    val sheetNameDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss")
    val newSheetTitle              = s"Main_${applicationStartTime.format(sheetNameDateTimeFormatter)}"
    val renameSheetRequest = new Request().setUpdateSheetProperties(
      new UpdateSheetPropertiesRequest()
        .setProperties(
          new SheetProperties().setSheetId(newSheetId).setTitle(newSheetTitle)
        )
        .setFields("title")
    )

    val deleteSheetsRequests = startingSheetsInfo
      .filter(_.getTitle.startsWith("Main_"))
      .sortBy(_.getTitle)(Ordering[String].reverse)
      .drop(nVocabularyBackupsToKeep - 1)
      .map(_.getSheetId)
      .map { obsoleteSheetId =>
        new Request().setDeleteSheet(new DeleteSheetRequest().setSheetId(obsoleteSheetId))
      }

    val batchUpdateRequestBody =
      new BatchUpdateSpreadsheetRequest()
        .setRequests((deleteSheetsRequests :+ renameSheetRequest).asJava)
        .setIncludeSpreadsheetInResponse(false)
    val batchUpdateResponse =
      spreadsheetsService.spreadsheets().batchUpdate(vocabularySpreadsheetId, batchUpdateRequestBody).execute()

    println("Updated")
    //    println(s"Querying spreadsheet $vocabularySpreadsheetId")
//
//    val schema       = Encoders.product[SheetRow].schema
//    val schemaNames  = schema.map(_.name)
//    val types        = schema.map(_.dataType)
//    val lastColIndex = schema.size - 1
//
//    val lastColSheetIndex = "A" * (lastColIndex / nSheetIndexes) + ('A' + lastColIndex % nSheetIndexes).toChar
//    val sheetRange        = s"${sheetName}!A:${lastColSheetIndex}"
//
//    println(s"Reading values range ${sheetRange}")
//
//    val vocabularyValues =
//      spreadsheetsService
//        .spreadsheets()
//        .values()
//        .get(vocabularySpreadsheetId, sheetRange)
//        .setValueRenderOption("FORMATTED_VALUE")
//        .setDateTimeRenderOption("FORMATTED_STRING")
//        .setPrettyPrint(true)
//        .execute()
//
//    val spark       = SparkSession.builder().appName("SheetsRead").master("local[*]").getOrCreate()
//    val sheetValues = vocabularyValues.getValues.asScala
//    val header      = sheetValues.head.asScala
//    val data        = sheetValues.tail
//
//    if (header != schemaNames) {
//      throw new RuntimeException("Schema names do not match")
//    }
//
//    val castedData = data.map(_.asScala.zip(types).map {
//      case (v: String, IntegerType)   => Integer.valueOf(v)
//      case (v: String, TimestampType) => Timestamp.valueOf(v)
//      case (v, _)                     => v
//    })
//    val df = spark.createDataFrame(spark.sparkContext.parallelize(castedData.map(r => Row.fromSeq(r))), schema)
//
//    val updatedDf = df
//      .withColumn("id", col("id") * 10)
//      .withColumn("updatedAt", date_format(col("updatedAt"), "yyyy-mm-dd HH:mm:dd"))
//      .select(schemaNames.map(col): _*)
//    val updatedData = updatedDf.collect().map(_.toSeq.map(_.asInstanceOf[AnyRef]).asJava).toSeq.asJava
//    val updateBody  = new ValueRange().setValues(updatedData)
//    val updateRange = s"UpdateTest!A2:$lastColSheetIndex"
//
//
//
//
//
//    val updateResult =
//      spreadsheetsService
//        .spreadsheets()
//        .values()
//        .update(vocabularySpreadsheetId, updateRange, updateBody)
//        .setValueInputOption("USER_ENTERED")
////        .setValueInputOption("RAW")
//        .execute()
//
//    System.out.printf("%d cells updated.", updateResult.getUpdatedCells)

  }

}
