package pb.dictionary.extraction.publish

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.silver.DefinedText
import pb.dictionary.extraction.{AreaUtils, CsvSnapshotsArea}

import java.sql.Timestamp

// TODO: write directly to Google Docs? Export csv programmatically? Rename output file?
class ManualEnrichmentArea(
    path: String,
    timestampProvider: () => Timestamp
) extends CsvSnapshotsArea[UndefinedText](path, timestampProvider) {
  import FinalPublishProduct._
  import UndefinedText._
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
        AreaUtils.timestampToString(col(FIRST_OCCURRENCE)) as FIRST_OCCURRENCE,
        AreaUtils.timestampToString(col(LATEST_OCCURRENCE)) as LATEST_OCCURRENCE,
      )
  }

  private def unpublished[T <: FinalPublishProduct](publishSnapshot: Dataset[T])(undefined: DataFrame) = {
    val unpublished = undefined.join(publishSnapshot, col(FORMS).contains(col(TEXT)), "left_anti")
    unpublished
  }
}
