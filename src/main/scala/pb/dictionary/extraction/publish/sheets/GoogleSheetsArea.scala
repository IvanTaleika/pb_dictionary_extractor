package pb.dictionary.extraction.publish.sheets

import com.google.api.services.drive.Drive
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import pb.dictionary.extraction.{Area, InvalidAreaStateException, ProductCompanion}

import java.nio.file.Paths
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

abstract class GoogleSheetsArea[Out <: Product: ProductCompanion] extends Area[Out] {
  import spark.implicits._
  import areaDescriptor.implicits._

  protected def driveService: Drive
  protected def spreadsheetsService: Sheets
  protected def timestampProvider: () => Timestamp
  protected def nBackupSheetsToKeep: Int

  protected val NSheetLetterIndexes = 'Z' - 'A' + 1
  protected val lastSheetIndex = {
    val lastSchemaIndex = schema.size - 1
    "A" * (lastSchemaIndex / NSheetLetterIndexes) + ('A' + lastSchemaIndex % NSheetLetterIndexes).toChar
  }

  private val parsedPath                                         = Paths.get(path)
  protected val sheetName                                        = parsedPath.getFileName.toString
  protected val backupSheetPrefix                                = s"${sheetName}_"
  protected val spreadsheetPath                                  = parsedPath.getParent
  protected val spreadsheetName                                  = spreadsheetPath.getFileName.toString
  protected val spreadsheetDirectory                             = spreadsheetPath.getParent.toString
  protected val (spreadsheetDirectoryId, spreadsheetId, sheetId) = connectToTheSpreadsheet()

  override def snapshot: Dataset[Out] = {
    val sheetDataRange = s"${sheetName}!A:${lastSheetIndex}"
    val sheetData      = querySheetData(spreadsheetId, sheetDataRange)
    val sheetHeader    = sheetData.head
    val sheetValues    = sheetData.tail

    logger.info(s"Fetched `${sheetValues.size}` rows of values.")

    val expectedNames = schema.map(_.name)
    val expectedTypes = schema.map(_.dataType)

    if (sheetHeader != expectedNames) {
      throw InvalidAreaStateException(s"Header `${sheetHeader
        .mkString(", ")}` does not match expected schema names `${expectedNames
        .mkString(", ")}` in the vocabulary Google sheet `${path}` ")
    }

    val castedData = sheetValues.zipWithIndex.map {
      case (row, i) =>
        if (row.size > schema.size) {
          throw InvalidAreaStateException(
            s"Row `${i + 1}` has unexpected number of columns `${row.size}` " +
              s"that is more than expected schema size `${schema.size}` " +
              s"in the vocabulary Google sheet `${path}`.")
        }
        row.padTo(schema.size, "").zip(expectedTypes).map {
          case (v: String, IntegerType)   => Integer.valueOf(v)
          case (v: String, TimestampType) => Timestamp.valueOf(v)
          case (v, _)                     => v
        }
    }

    spark.createDataFrame(spark.sparkContext.parallelize(castedData.map(r => Row.fromSeq(r))), schema).as[Out]
  }

  private def connectToTheSpreadsheet() = {
    val directoryId      = queryDirectoryId(spreadsheetDirectory)
    val spreadsheetId    = querySpreadsheetId(spreadsheetName, directoryId)
    val sheetsProperties = querySheetsProperties(spreadsheetId)
    val sheetId          = sheetsProperties.find(_.getTitle == sheetName).map(_.getSheetId).get

    (directoryId, spreadsheetId, sheetId)
  }

  private def queryDirectoryId(name: String) = {
    logger.info(s"Querying for ID of the spreadsheet directory `$name`")
    val directoryId = listDriveFiles()
      .setQ(s"mimeType = 'application/vnd.google-apps.folder' and name = '$name'")
      .setFields("files(id)")
      .execute()
      .getFiles
      .asScala
      .head
      .getId
    logger.info(s"Spreadsheet directory `$name` ID is `$directoryId`")
    directoryId
  }

  private def querySpreadsheetId(name: String, directoryId: String) = {
    logger.info(s"Querying for ID of the spreadsheet `${name}` from directory `$directoryId`")
    val spreadsheetId = listDriveFiles()
      .setQ(
        s"mimeType='application/vnd.google-apps.spreadsheet' and " +
          s"name = '$name' and " +
          s"'${directoryId}' in parents")
      .setFields("files(id)")
      .execute()
      .getFiles
      .asScala
      .head
      .getId
    logger.info(s"Spreadsheet `${name}` from directory `$directoryId` ID is `$spreadsheetId`")
    spreadsheetId
  }

  private def querySheetsProperties(spreadsheetId: String) = {
    logger.info(s"Querying for sheets the spreadsheet `${spreadsheetId}`")
    spreadsheetsService
      .spreadsheets()
      .get(spreadsheetId)
      .setIncludeGridData(false)
      .execute()
      .getSheets
      .asScala
      .map(_.getProperties)
  }

  private def querySheetData(spreadsheetId: String, range: String) = {
    logger.info(s"Querying sheet `$range` data from the spreadsheet `$spreadsheetId`")

    spreadsheetsService
      .spreadsheets()
      .values()
      .get(spreadsheetId, range)
      .setValueRenderOption("FORMATTED_VALUE")
      .setDateTimeRenderOption("FORMATTED_STRING")
      .setPrettyPrint(true)
      .execute()
      .getValues
      .asScala
      .map(_.asScala)
  }

  private def listDriveFiles() = driveService.files().list().setSpaces("drive")

  protected def write(preUpsertSnapshot: Dataset[Out], postUpsertSnapshot: Dataset[Out]): Dataset[Out] = {
    // DF can be transformed to DS regarding the column order. Collected DS is always in the same order as its Product type
    write(preUpsertSnapshot, postUpsertSnapshot.collect().toSeq)
  }

  protected def write(preUpsertSnapshot: Dataset[Out], postUpsertSnapshot: Seq[Out]): Dataset[Out] = {
    logger.info(s"Creating requests for transactional backup and write on the spreadsheet `$path`")

    val currentTimestamp   = timestampProvider()
    val sheetNameTimestamp = currentTimestamp.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss"))
    val backupSheetName    = s"$backupSheetPrefix${sheetNameTimestamp}"

    val existingSheets  = querySheetsProperties(spreadsheetId)
    val nExistingSheets = existingSheets.size

    val backupSheetRequests   = createBackupSheetRequests(backupSheetName, nExistingSheets)
    val deleteSheetsRequests  = createDeleteObsoleteBackupsRequests(existingSheets)
    val expandSheetRequests   = createExpandSheetRequests(preUpsertSnapshot)(postUpsertSnapshot)
    val updateDataRequests    = createDataUpdateRequests(postUpsertSnapshot)
    val transactionalRequests = backupSheetRequests ++ expandSheetRequests ++ updateDataRequests ++ deleteSheetsRequests

    val batchUpdateRequestBody = new BatchUpdateSpreadsheetRequest()
      .setRequests(transactionalRequests.asJava)
      .setIncludeSpreadsheetInResponse(false)

    logger.info(s"Running created requests in a single transactional update on a spreadsheet `${spreadsheetId}`")
    spreadsheetsService.spreadsheets().batchUpdate(spreadsheetId, batchUpdateRequestBody).execute()
    logger.info(s"Sheet `$path` is updated successfully.")

    snapshot
  }

  private def createBackupSheetRequests(backupSheetName: String, nExistingSheets: Int) = {
    val backupSheetRequest = new Request().setDuplicateSheet(
      new DuplicateSheetRequest()
        .setSourceSheetId(sheetId)
        .setNewSheetName(backupSheetName)
        .setInsertSheetIndex(nExistingSheets)
    )
    logger.info(s"Created sheet `$sheetName` duplicate request to a name `${backupSheetName}`")
    Seq(backupSheetRequest)
  }

  private def createDeleteObsoleteBackupsRequests(existingSheets: Seq[SheetProperties]) = {
    val sheetsToDelete = existingSheets
      .filter(_.getTitle.startsWith(backupSheetPrefix))
      .sortBy(_.getTitle)(Ordering[String].reverse)
      .drop(nBackupSheetsToKeep - 1)

    val deleteSheetsRequests = sheetsToDelete
      .map(_.getSheetId)
      .map { obsoleteSheetId =>
        new Request().setDeleteSheet(new DeleteSheetRequest().setSheetId(obsoleteSheetId))
      }
    logger.info(
      s"Created requests for `${sheetsToDelete.map(_.getTitle).mkString(", ")}` sheets deletion" +
        s" to keep at most `${nBackupSheetsToKeep}` backup sheets.")
    deleteSheetsRequests
  }

  private def createExpandSheetRequests(preUpsertSnapshot: Dataset[Out])(postUpsertSnapshot: Seq[Out]) = {
    val nAppendRows = (postUpsertSnapshot.size - preUpsertSnapshot.count()).toInt
    if (nAppendRows > 0) {
      val appendRowsRequest = new Request().setAppendDimension(
        new AppendDimensionRequest().setSheetId(sheetId).setDimension("ROWS").setLength(nAppendRows)
      )
      Seq(appendRowsRequest)
    } else {
      Seq.empty
    }
  }

  private def createDataUpdateRequests(postUpsertSnapshot: Seq[Out]) = {
    val sheetRowUpdates = postUpsertSnapshot
      .map(
        sheetRow =>
          sheetRow.productIterator
            .collect {
              case v: Int    => new ExtendedValue().setNumberValue(v)
              case v: String => new ExtendedValue().setStringValue(v)
            }
            .map(v => new CellData().setUserEnteredValue(v))
            .toSeq
      )
      .map(vs => new RowData().setValues(vs.asJava))
      .asJava

    val sheetUpdateRange = new GridRange()
      .setSheetId(sheetId)
      .setStartColumnIndex(0)
      .setEndColumnIndex(schema.size)
      .setStartRowIndex(1)

    val updateDataRequest = new Request().setUpdateCells(
      new UpdateCellsRequest().setRows(sheetRowUpdates).setRange(sheetUpdateRange).setFields("userEnteredValue")
    )
    logger.info(s"Created requests to update `${postUpsertSnapshot.size}` rows of data in sheet `$path`.")
    Seq(updateDataRequest)
  }
}
