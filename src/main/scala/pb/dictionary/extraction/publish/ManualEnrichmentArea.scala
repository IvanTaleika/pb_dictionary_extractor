package pb.dictionary.extraction.publish

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.silver.DefinedText
import pb.dictionary.extraction.CsvSnapshotsArea

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

// TODO: write directly to Google Docs? Export csv programmatically? Rename output file?
class ManualEnrichmentArea(
    path: String,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends CsvSnapshotsArea[UndefinedText](path, timestampProvider) {
  import UndefinedText._
  import FinalPublishProduct._
  override protected def outputFiles = Option(1)

  def upsert[T <: FinalPublishProduct](
      silverSnapshot: Dataset[DefinedText],
      publishSnapshot: Dataset[T]
  ): Dataset[UndefinedText] = {
    silverSnapshot.transform(fromSilver).transform(unpublished(publishSnapshot)).transform(write)
  }

  private def fromSilver(silverSnapshot: Dataset[DefinedText]) = {
    silverSnapshot
      .filter(col(DefinedText.DEFINITION).isNull)
      .select(
        col(TEXT),
        col(OCCURRENCES),
        array_join(col(BOOKS), ", ") as BOOKS,
        timestampToCsvString(col(FIRST_OCCURRENCE)) as FIRST_OCCURRENCE,
        timestampToCsvString(col(LATEST_OCCURRENCE)) as LATEST_OCCURRENCE,
      )
  }

  private def unpublished[T <: FinalPublishProduct](publishSnapshot: Dataset[T])(undefined: DataFrame) = {
    val unpublished = undefined.join(publishSnapshot, col(FORMS).contains(col(TEXT)), "left_anti")
    unpublished
  }
}
