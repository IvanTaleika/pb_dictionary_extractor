package pb.dictionary.extraction.publish.sheets

import com.google.api.services.drive.Drive
import com.google.api.services.sheets.v4.Sheets
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.AreaUtils
import pb.dictionary.extraction.golden.DictionaryRecord
import pb.dictionary.extraction.golden.DictionaryRecord._

import java.sql.Timestamp
import java.util.regex.Pattern

class SheetsPublishArea(
    val path: String,
    protected val driveService: Drive,
    protected val spreadsheetsService: Sheets,
    protected val timestampProvider: () => Timestamp,
    protected val nBackupSheetsToKeep: Int = 5
) extends GoogleSheetsArea[VocabularyRow] {
  import VocabularyRow._
  import spark.implicits._

  protected val arrayRecordsSeparator = "\n* "


  /** Upsert records from lower tier area by PK and returns new records. */
  def upsert(previousSnapshot: Dataset[DictionaryRecord]): Dataset[VocabularyRow] = {
    val preUpsertSnapshot = snapshot.cache()
    previousSnapshot.transform(fromGolden(preUpsertSnapshot)).transform(write(preUpsertSnapshot, _))
  }

  private def fromGolden(preUpsertSnapshot: Dataset[VocabularyRow])(golden: Dataset[DictionaryRecord]) = {
    val goldenAlias              = "golden"
    val publishedAlias           = "published"
    def colPublished(cn: String) = col(s"${publishedAlias}.${cn}")
    def colGolden(cn: String)    = col(s"${goldenAlias}.${cn}")

    val mergedStates = preUpsertSnapshot
      .as(publishedAlias)
      .join(golden.as(goldenAlias),
            DictionaryRecord.pk.map(cn => colPublished(cn) === colGolden(cn)).reduce(_ && _),
            "full_outer")
    // for calculated columns `collect` and `max().over()` generate the same DAG, except for
    // the last step where the value is either fetched to master or broadcasted. However, in case the column
    // is already stored in parquet file, collect can efficiently fetch it using metadata, while WF issues
    // shuffle and actual value search
    val biggestSnapshotId = preUpsertSnapshot.select(ID).as[Int].orderBy(col(ID).desc).head(1).headOption.getOrElse(0)
    val rnCol             = "rowNumber"
    val newSheet = mergedStates
    // must run row_number after the join to exclude matched rows
      .withColumn(rnCol,
                  row_number().over(Window.partitionBy(ID).orderBy(colGolden(DictionaryRecord.FIRST_OCCURRENCE))))
      .select(
        // attributes, not present in Golden area must be selected from the Sheet if present
        coalesce(colPublished(ID), col(rnCol) + lit(biggestSnapshotId)) as ID,
        coalesce(colPublished(STATUS), lit(NewStatus)) as STATUS,
        // Natural PK can be selected in any order
        coalesce(colPublished(NORMALIZED_TEXT), colGolden(NORMALIZED_TEXT)) as NORMALIZED_TEXT,
        coalesce(colPublished(DEFINITION), colGolden(DEFINITION)) as DEFINITION,
        // string attributes may be fixed manually, Sheet data are in priority
        coalesceEmptyString(colPublished(PART_OF_SPEECH), colGolden(PART_OF_SPEECH)) as PART_OF_SPEECH,
        coalesceEmptyString(colPublished(PHONETIC), colGolden(PHONETIC)) as PHONETIC,
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
        coalesceEmptyString(mergeArrayAttributes(colPublished(TRANSLATIONS), colGolden(TRANSLATIONS))) as TRANSLATIONS,
        // Non-existing Golden columns
        coalesceEmptyString(colPublished(TAGS)) as TAGS,
        coalesceEmptyString(colPublished(NOTES)) as NOTES
      )

    newSheet.as[VocabularyRow]
  }

  private def coalesceEmptyString(cols: Column*) = {
    coalesce((cols :+ lit("")): _*)
  }

  private def mergeArrayAttributes(publishCol: Column, goldenCol: Column) = {
    val splitPattern = Pattern.quote(arrayRecordsSeparator)

    val publishNotNullCol = coalesce(split(publishCol, splitPattern), lit(Array.empty[String]))
    val goldenNotNullCol  = coalesce(goldenCol, lit(Array.empty[String]))
    array_join(array_union(publishNotNullCol, goldenNotNullCol), arrayRecordsSeparator)
  }

  override protected def write(preUpsertSnapshot: Dataset[VocabularyRow], postUpsertSnapshot: Dataset[VocabularyRow]): Dataset[VocabularyRow] = {
    write(preUpsertSnapshot, postUpsertSnapshot.collect().toSeq.sortBy(_.id))
  }
}
