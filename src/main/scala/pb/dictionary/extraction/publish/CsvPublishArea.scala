package pb.dictionary.extraction.publish

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.CsvSnapshotsArea
import pb.dictionary.extraction.golden.DictionaryRecord
import pb.dictionary.extraction.golden.DictionaryRecord._

import java.sql.Timestamp

// We can also use Google API in foreach/map function https://developers.google.com/sheets/api/guides/values
// or excel spark connector https://github.com/crealytics/spark-excel
class CsvPublishArea(
    path: String,
    timestampProvider: () => Timestamp
) extends CsvSnapshotsArea[CsvRow](path, timestampProvider) {
  import CsvRow._
  import spark.implicits._
  // FIXME: this will work badly with examples column
  private val arrayJoin = ", "

  override protected def outputFiles = Option(1)

  /** Upsert records from lower tier area by PK and returns new records. */
  def upsert(previousSnapshot: Dataset[DictionaryRecord]): Dataset[CsvRow] = {
    previousSnapshot.transform(fromGolden).transform(write)
  }

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
      .withColumn(rnCol, row_number().over(Window.partitionBy(ID).orderBy(colGolden(DictionaryRecord.FIRST_OCCURRENCE))))
      .select(
        // select by column type and meaning, but not final order

        // attributes, not present in Golden area must be selected from the Sheet if present
        coalesce(colPublished(ID), col(rnCol) + lit(biggestSnapshotId)) as ID,
        coalesce(colPublished(STATUS), lit(NewStatus)) as STATUS,
        // Natural PK can be selected in any order
        coalesce(colPublished(NORMALIZED_TEXT), colGolden(NORMALIZED_TEXT)) as NORMALIZED_TEXT,
        coalesce(colPublished(DEFINITION), colGolden(DEFINITION)) as DEFINITION,
        // string attributes may be fixed manually, Sheet data are in priority
        coalesce(colPublished(PART_OF_SPEECH), colGolden(PART_OF_SPEECH)) as PART_OF_SPEECH,
        coalesce(colPublished(PHONETIC), colGolden(PHONETIC)) as PHONETIC,
        coalesce(colPublished(TRANSLATION), colGolden(TRANSLATION)) as TRANSLATION,
        coalesce(colPublished(USAGE), concat(format_number(colGolden(USAGE) * 100, UsageDecimals), lit("%"))) as USAGE,
        // Attributes that can be updated from the device. Golden area values are in priority to reflect the latest attributes state
        coalesce(colGolden(OCCURRENCES), colPublished(OCCURRENCES)) as OCCURRENCES,
        coalesce(timestampToCsvString(colGolden(FIRST_OCCURRENCE)), colPublished(FIRST_OCCURRENCE)) as FIRST_OCCURRENCE,
        coalesce(timestampToCsvString(colGolden(LATEST_OCCURRENCE)), colPublished(LATEST_OCCURRENCE)) as LATEST_OCCURRENCE,
        // Arrays can be merged
        mergeArrayAttributes(colPublished(FORMS), colGolden(FORMS)) as FORMS,
        mergeArrayAttributes(colPublished(SOURCES), colGolden(BOOKS)) as SOURCES,
        mergeArrayAttributes(colPublished(EXAMPLES), colGolden(EXAMPLES)) as EXAMPLES,
        mergeArrayAttributes(colPublished(SYNONYMS), colGolden(SYNONYMS)) as SYNONYMS,
        mergeArrayAttributes(colPublished(ANTONYMS), colGolden(ANTONYMS)) as ANTONYMS,
      )
      // select by correct GoogleSheet order
      .select(attributesOrder.map(col): _*)
    newSheet
  }

  private def mergeArrayAttributes(publishCol: Column, goldenCol: Column) = {
    val publishNotNullCol = coalesce(split(publishCol, arrayJoin), lit(Array.empty[String]))
    val goldenNotNullCol  = coalesce(goldenCol, lit(Array.empty[String]))
    array_join(array_union(publishNotNullCol, goldenNotNullCol), arrayJoin)
  }
}
