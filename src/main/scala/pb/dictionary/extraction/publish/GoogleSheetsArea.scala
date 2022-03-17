package pb.dictionary.extraction.publish

import com.google.api.services.drive.Drive
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model._
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import pb.dictionary.extraction.{Area, AreaUtils}
import pb.dictionary.extraction.golden.DictionaryRecord
import pb.dictionary.extraction.golden.DictionaryRecord._

import java.nio.file.Paths
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

class GoogleSheetsArea(
    val path: String,
    driveService: Drive,
    spreadsheetsService: Sheets,
    timestampProvider: () => Timestamp,
    nBackupSheetsToKeep: Int = 5
) extends Area[SheetRow] {
  import SheetRow._
  import spark.implicits._

  private val NSheetLetterIndexes = 'Z' - 'A' + 1
  private val lastSchemaIndex     = schema.size - 1
  private val lastSheetIndex      = "A" * (lastSchemaIndex / NSheetLetterIndexes) + ('A' + lastSchemaIndex % NSheetLetterIndexes).toChar

  private val parsedPath                                       = Paths.get(path)
  private val sheetName                                        = parsedPath.getFileName.toString
  private val backupSheetPrefix                                = s"${sheetName}_"
  private val spreadsheetPath                                  = parsedPath.getParent
  private val spreadsheetName                                  = spreadsheetPath.getFileName.toString
  private val spreadsheetDirectory                             = spreadsheetPath.getParent.toString
  private val (spreadsheetDirectoryId, spreadsheetId, sheetId) = connectToTheSpreadsheet()

  // FIXME: this will work badly with examples column
  private val arrayJoin = ", "

  override def snapshot: Dataset[SheetRow] = buildSnapshot.cache()

  private def buildSnapshot: Dataset[SheetRow] = {
    val sheetDataRange = s"${sheetName}!A:${lastSheetIndex}"
    val sheetData      = querySheetData(spreadsheetId, sheetDataRange)
    val sheetHeader    = sheetData.head
    val sheetValues    = sheetData.tail

    logger.info(s"Fetched `${sheetValues.size}` rows of values.")

    val expectedNames = schema.map(_.name)
    val expectedTypes = schema.map(_.dataType)

    if (sheetHeader != expectedNames) {
      // TODO: error handling
      throw new RuntimeException("Schema names do not match")
    }

    val castedData = sheetValues.map { row =>
      if (row.size > schema.size) {
        // TODO: error handling
        throw new RuntimeException("Row size does not match")
      }
      row.padTo(schema.size, "").zip(expectedTypes).map {
        case (v: String, IntegerType)   => Integer.valueOf(v)
        case (v: String, TimestampType) => Timestamp.valueOf(v)
        case (v, _)                     => v
      }
    }

    spark.createDataFrame(spark.sparkContext.parallelize(castedData.map(r => Row.fromSeq(r))), schema).as[SheetRow]
  }

  /** Upsert records from lower tier area by PK and returns new records. */
  def upsert(previousSnapshot: Dataset[DictionaryRecord]): Dataset[SheetRow] = {
    previousSnapshot.transform(fromGolden).transform(write)
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

  private def fromGolden(golden: Dataset[DictionaryRecord]) = {
    val goldenAlias              = "golden"
    val publishedAlias           = "published"
    def colPublished(cn: String) = col(s"${publishedAlias}.${cn}")
    def colGolden(cn: String)    = col(s"${goldenAlias}.${cn}")
    val mergedStates = snapshot
      .as(publishedAlias)
      .join(golden.as(goldenAlias),
            DictionaryRecord.pk.map(cn => colPublished(cn) === colGolden(cn)).reduce(_ && _),
            "full_outer")
    // for calculated columns `collect` and `max().over()` generate the same DAG, except for
    // the last step where the value is either fetched to master or broadcasted. However, in case the column
    // is already stored in parquet file, collect can efficiently fetch it using metadata, while WF issues
    // shuffle and actual value search
    val biggestSnapshotId = snapshot.select(ID).as[Int].orderBy(col(ID).desc).head(1).headOption.getOrElse(0)
    val rnCol             = "rowNumber"
    val newSheet = mergedStates
    // must run row_number after the join to exclude matched rows
      .withColumn(rnCol,
                  row_number().over(Window.partitionBy(ID).orderBy(colGolden(DictionaryRecord.FIRST_OCCURRENCE))))
      .select(
        // select by column type and meaning, but not final order

        // attributes, not present in Golden area must be selected from the Sheet if present
        coalesce(colPublished(ID), col(rnCol) + lit(biggestSnapshotId)) as ID,
        coalesce(colPublished(STATUS), lit(NewStatus)) as STATUS,
        // Natural PK can be selected in any order
        coalesce(colPublished(NORMALIZED_TEXT), colGolden(NORMALIZED_TEXT)) as NORMALIZED_TEXT,
        coalesce(colPublished(DEFINITION), colGolden(DEFINITION)) as DEFINITION,
        // string attributes may be fixed manually, Sheet data are in priority
        coalesceEmptyString(colPublished(PART_OF_SPEECH), colGolden(PART_OF_SPEECH)) as PART_OF_SPEECH,
        coalesceEmptyString(colPublished(PHONETIC), colGolden(PHONETIC)) as PHONETIC,
        coalesceEmptyString(colPublished(TRANSLATION), colGolden(TRANSLATION)) as TRANSLATION,
        coalesceEmptyString(colPublished(USAGE), concat(format_number(colGolden(USAGE) * 100, UsageDecimals), lit("%"))) as USAGE,
        // Attributes that can be updated from the device. Golden area values are in priority to reflect the latest attributes state
        coalesce(colGolden(OCCURRENCES), colPublished(OCCURRENCES), lit(1)) as OCCURRENCES,
        coalesceEmptyString(AreaUtils.timestampToString(colGolden(FIRST_OCCURRENCE)), colPublished(FIRST_OCCURRENCE)) as FIRST_OCCURRENCE,
        coalesceEmptyString(AreaUtils.timestampToString(colGolden(LATEST_OCCURRENCE)), colPublished(LATEST_OCCURRENCE)) as LATEST_OCCURRENCE,
        // Arrays can be merged
        coalesceEmptyString(mergeArrayAttributes(colPublished(FORMS), colGolden(FORMS))) as FORMS,
        coalesceEmptyString(mergeArrayAttributes(colPublished(SOURCES), colGolden(BOOKS))) as SOURCES,
        coalesceEmptyString(mergeArrayAttributes(colPublished(EXAMPLES), colGolden(EXAMPLES))) as EXAMPLES,
        coalesceEmptyString(mergeArrayAttributes(colPublished(SYNONYMS), colGolden(SYNONYMS))) as SYNONYMS,
        coalesceEmptyString(mergeArrayAttributes(colPublished(ANTONYMS), colGolden(ANTONYMS))) as ANTONYMS,
        // Non-existing Golden columns
        coalesceEmptyString(colPublished(TAGS)) as TAGS,
        coalesceEmptyString(colPublished(NOTES)) as NOTES
      )
      // select by correct GoogleSheet order
      .select(attributesOrder.map(col): _*)
    newSheet.as[SheetRow]
  }

  private def coalesceEmptyString(cols: Column*) = {
    coalesce((cols :+ lit("")): _*)
  }

  private def mergeArrayAttributes(publishCol: Column, goldenCol: Column) = {
    val publishNotNullCol = coalesce(split(publishCol, arrayJoin), lit(Array.empty[String]))
    val goldenNotNullCol  = coalesce(goldenCol, lit(Array.empty[String]))
    array_join(array_union(publishNotNullCol, goldenNotNullCol), arrayJoin)
  }

  private def write(df: Dataset[SheetRow]): Dataset[SheetRow] = {
    logger.info(s"Creating requests for transactional backup and write on the spreadsheet `$path`")

    val currentTimestamp   = timestampProvider()
    val sheetNameTimestamp = currentTimestamp.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss"))
    val backupSheetName    = s"$backupSheetPrefix${sheetNameTimestamp}"

    val existingSheets  = querySheetsProperties(spreadsheetId)
    val nExistingSheets = existingSheets.size

    val backupSheetRequests   = createBackupSheetRequests(backupSheetName, nExistingSheets)
    val deleteSheetsRequests  = createDeleteObsoleteBackupsRequests(existingSheets)
    val expandSheetRequests   = createExpandSheetRequests(df)
    val updateDataRequests    = createDataUpdateRequests(df)
    val transactionalRequests = backupSheetRequests ++ expandSheetRequests ++ updateDataRequests ++ deleteSheetsRequests

    val batchUpdateRequestBody = new BatchUpdateSpreadsheetRequest()
      .setRequests(transactionalRequests.asJava)
      .setIncludeSpreadsheetInResponse(false)

    logger.info(s"Running created requests in a single transactional update on a spreadsheet `${spreadsheetId}`")
    spreadsheetsService.spreadsheets().batchUpdate(spreadsheetId, batchUpdateRequestBody).execute()
    logger.info(s"Sheet `$path` is updated successfully.")

    // TODO: check if DF is recalculated only after the update
    buildSnapshot.unpersist()
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

  private def createExpandSheetRequests(df: Dataset[SheetRow]) = {
    val nAppendRows = (df.count() - snapshot.count()).toInt
    if (nAppendRows > 0) {
      val appendRowsRequest = new Request().setAppendDimension(
        new AppendDimensionRequest().setSheetId(sheetId).setDimension("ROWS").setLength(nAppendRows)
      )
      Seq(appendRowsRequest)
    } else {
      Seq.empty
    }
  }

  private def createDataUpdateRequests(df: Dataset[SheetRow]) = {
    val collectedDf = df.collect().toSeq

    val sheetRowUpdates = collectedDf
      .sortBy(_.id)
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
    logger.info(s"Created requests to update `${collectedDf.size}` rows of data in sheet `$path`.")
    Seq(updateDataRequest)
  }
}
