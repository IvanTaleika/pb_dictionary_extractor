package pb.dictionary.extraction.stage

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode}
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.{ApplicationManagedArea, CsvSnapshotsArea}
import pb.dictionary.extraction.device.DeviceHighlight

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

class StageArea(
    path: String,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends CsvSnapshotsArea[DeviceHighlight, HighlightedText](path, timestampProvider) {
  import HighlightedText._
  import spark.implicits._

  override def upsert(previousSnapshot: Dataset[DeviceHighlight]): Dataset[HighlightedText] = {
    previousSnapshot.transform(findUpdates).transform(fromUserHighlights).transform(writeSnapshot)
  }

  override protected def findUpdates(previousSnapshot: Dataset[DeviceHighlight]) = {
    // TODO: check filter pushdowns when working with DB
    previousSnapshot.where(col(OID) > lit(latestOid))
  }

  private def fromUserHighlights(userHighlights: Dataset[DeviceHighlight]) = {
    import DeviceHighlight._
    val parsedValueCol = "parsedValue"
    userHighlights
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
    snapshot.select(OID).orderBy(col(OID).desc).as[Long].collect().headOption.getOrElse(Integer.MIN_VALUE)

}
