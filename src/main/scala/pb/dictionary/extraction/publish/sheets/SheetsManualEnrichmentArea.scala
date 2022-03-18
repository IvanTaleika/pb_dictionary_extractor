package pb.dictionary.extraction.publish.sheets

import com.google.api.services.drive.Drive
import com.google.api.services.sheets.v4.Sheets
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.{AreaUtils, CsvSnapshotsArea}
import pb.dictionary.extraction.publish.FinalPublishProduct
import pb.dictionary.extraction.silver.DefinedText

import java.sql.Timestamp

class SheetsManualEnrichmentArea(
    val path: String,
    protected val driveService: Drive,
    protected val spreadsheetsService: Sheets,
    protected val timestampProvider: () => Timestamp,
    protected val nBackupSheetsToKeep: Int = 0
) extends GoogleSheetsArea[UndefinedRow] {
  import UndefinedRow._
  import spark.implicits._

  def upsert[T <: FinalPublishProduct](
      silverSnapshot: Dataset[DefinedText],
      publishSnapshot: Dataset[T]
  ): Dataset[UndefinedRow] = {
    val cachedSnapshot = snapshot.cache()
    silverSnapshot.transform(fromSilver).transform(unpublished(publishSnapshot)).transform(write(cachedSnapshot, _))
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
      .as[UndefinedRow]
  }

  private def unpublished[T <: FinalPublishProduct](publishSnapshot: Dataset[T])(
      undefined: Dataset[UndefinedRow]) = {
    val unpublished = undefined.join(publishSnapshot, col(FinalPublishProduct.FORMS).contains(col(TEXT)), "left_anti")
    unpublished.as[UndefinedRow]
  }
}
