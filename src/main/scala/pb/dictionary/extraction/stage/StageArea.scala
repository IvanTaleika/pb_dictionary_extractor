package pb.dictionary.extraction.stage

import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.CsvArea
import pb.dictionary.extraction.device.PocketBookMark

import java.sql.Timestamp

/** Stores PocketBook highlight marks, created by flattening and filtering PocketBook DB mark entries.
  * The area inherits [[PocketBookMark.pk]] primary key, expecting them to be unique and always increasing.
  * The goal of this area is to keep highlights info in a simple human-readable format.
  * The area stores data in a managed spark metastore table backed by incremental CSV files.
  */
class StageArea(
    path: String,
    timestampProvider: () => Timestamp
) extends CsvArea[HighlightedText](path, timestampProvider) {
  import HighlightedText._
  import spark.implicits._

  def upsert(previousAreaSnapshot: Dataset[PocketBookMark]): Dataset[HighlightedText] = {
    previousAreaSnapshot.transform(findUpdates).transform(fromPocketBookMarks).transform(write)
  }

  private def findUpdates(previousAreaSnapshot: Dataset[PocketBookMark]) = {
    // TODO: check filter pushdowns when working with DB
    previousAreaSnapshot.where(col(OID) > lit(latestOid))
  }

  /** Flattens and filters PocketBook mark structures, keeping highlights only.  */
  private def fromPocketBookMarks(userMarks: Dataset[PocketBookMark]) = {
    import PocketBookMark._
    val parsedValueCol = "parsedValue"
    userMarks
      .withColumn(parsedValueCol, from_json(col(VAL), Encoders.product[HighlightInfo].schema))
      // Bookmarks does not have begin/end attributes
      .where(col(parsedValueCol)(HighlightInfo.BEGIN).isNotNull)
      .select(
        col(OID),
        col(parsedValueCol)(HighlightInfo.TEXT) as HighlightedText.TEXT,
        col(TITLE),
        col(AUTHORS),
        col(TIME_EDT)
      )
  }

  private def latestOid: Long =
    snapshot.select(OID).orderBy(col(OID).desc).as[Long].collect().headOption.getOrElse(Long.MinValue)

}
