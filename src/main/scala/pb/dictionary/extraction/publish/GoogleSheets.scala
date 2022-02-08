package pb.dictionary.extraction.publish

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.CsvSnapshotsArea
import pb.dictionary.extraction.device.DeviceHighlight
import pb.dictionary.extraction.golden.DictionaryRecord
import pb.dictionary.extraction.golden.DictionaryRecord._
import pb.dictionary.extraction.stage.HighlightedText

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
// We can also use Google API in foreach/map function https://developers.google.com/sheets/api/guides/values
// or excel spark connector https://github.com/crealytics/spark-excel
class GoogleSheets(
    path: String,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends CsvSnapshotsArea[DictionaryRecord, HighlightedText](path, timestampProvider) {

  override protected def outputFiles = Option(1)

  /** Upsert records from lower tier area by PK and returns new records. */
  override def upsert(previousSnapshot: Dataset[DictionaryRecord]): Dataset[HighlightedText] = {
    previousSnapshot.transform(fromGolden).transform(writeSnapshot)
  }

  private def fromGolden(golden: Dataset[DictionaryRecord]) = {
    val newRowStatus  = "new"
    val usageDecimals = 6

    import SheetRow._
    import spark.implicits._
    val goldenAlias              = "published"
    val publishedAlias           = "published"
    def colPublished(cn: String) = col(s"${publishedAlias}.${cn}")
    def colGolden(cn: String)    = col(s"${goldenAlias}.${cn}")
    val mergedStates = snapshot
      .as(publishedAlias)
      .join(golden.as(goldenAlias),
            DictionaryRecord.pk.map(cn => colPublished(cn) === colGolden(cn)).reduce(_ && _),
            "fullOuter")
    // TODO: test what is better, collect or window?
    val biggestSnapshotId = snapshot.select(ID).as[Int].orderBy(col(ID).desc).head(1).headOption.getOrElse(0)
    val rnCol             = "rowNumber"
    val newSheet = mergedStates
      .withColumn(rnCol, row_number().over(Window.partitionBy(ID).orderBy(DictionaryRecord.FIRST_OCCURRENCE)))
      .select(
        // column order is important!
        coalesce(colPublished(ID), col(rnCol) + lit(biggestSnapshotId)) as ID,
        coalesce(colPublished(STATUS), lit(newRowStatus)) as STATUS,
        coalesce(colPublished(NORMALIZED_TEXT), colGolden(NORMALIZED_TEXT)) as NORMALIZED_TEXT,
        coalesce(colPublished(PART_OF_SPEECH), colGolden(PART_OF_SPEECH)) as PART_OF_SPEECH,
        coalesce(colPublished(PHONETIC), colGolden(PHONETIC)) as PHONETIC,
        coalesce(colPublished(FORMS), colGolden(FORMS)) as FORMS,
        coalesce(colPublished(SOURCE), colGolden(BOOKS)) as SOURCE,
        coalesce(colPublished(OCCURRENCES), colGolden(OCCURRENCES)) as OCCURRENCES,
        coalesce(colPublished(FIRST_OCCURRENCE), timestampToCsvString(colGolden(FIRST_OCCURRENCE))) as FIRST_OCCURRENCE,
        coalesce(colPublished(LATEST_OCCURRENCE), timestampToCsvString(colGolden(LATEST_OCCURRENCE))) as LATEST_OCCURRENCE,
        coalesce(colPublished(DEFINITION), colGolden(DEFINITION)) as DEFINITION,
        coalesce(colPublished(EXAMPLES), array_join(colGolden(EXAMPLES), ",")) as EXAMPLES,
        coalesce(colPublished(SYNONYMS), array_join(colGolden(SYNONYMS), ",")) as SYNONYMS,
        coalesce(colPublished(ANTONYMS), array_join(colGolden(ANTONYMS), ",")) as ANTONYMS,
        coalesce(colPublished(TRANSLATION), colGolden(TRANSLATION)) as TRANSLATION,
        coalesce(colPublished(USAGE), concat(format_number(colGolden(USAGE) * 100, usageDecimals), lit("%"))) as USAGE,
      )
    newSheet
  }

}
