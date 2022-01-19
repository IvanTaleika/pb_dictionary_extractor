package pb.dictionary.extraction.stage

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode}
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.ApplicationManagedArea
import pb.dictionary.extraction.device.DeviceHighlight

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

class StageArea(
    path: String,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends ApplicationManagedArea[DeviceHighlight, HighlightedSentence](path, "csv") {
  import HighlightedSentence._
  import spark.implicits._
  override protected def tableOptions    = Map("multiline" -> "true", "header" -> "true", "mode" -> "FAILFAST")
  override protected def tablePartitions = Seq(UPDATED_AT)

  override protected def initTable(): Unit = {
    super.initTable()
    spark.sql(s"msck repair table ${fullTableName}")
  }

  override def upsert(previousSnapshot: Dataset[DeviceHighlight]): Dataset[HighlightedSentence] = {
    previousSnapshot.transform(findUpdates).transform(fromUserHighlights).transform(writeNew)
  }

  override protected def findUpdates(previousSnapshot: Dataset[DeviceHighlight]) = {
    // TODO: check filter pushdowns when working with DB
    previousSnapshot.where(col(OID) > lit(latestOid))
  }

  private def fromUserHighlights(userHighlights: Dataset[DeviceHighlight]) = {
    val parsedValueCol = "parsedValue"
    userHighlights
      .withColumn(parsedValueCol, from_json(col(DeviceHighlight.VAL), Encoders.product[HighlightInfo].schema))
      // Bookmarks does not have begin/end attributes
      .where(col(parsedValueCol)(HighlightInfo.BEGIN).isNotNull)
      .select(
        col(DeviceHighlight.OID),
        col(parsedValueCol)(HighlightInfo.TEXT) as HighlightedSentence.TEXT,
        col(DeviceHighlight.TITLE),
        col(DeviceHighlight.AUTHORS),
        col(DeviceHighlight.TIME_EDT)
      )
  }

  private def writeNew(wordHighlights: DataFrame): Dataset[HighlightedSentence] = {
    val updateTimestamp = timestampProvider()

    wordHighlights
      .withColumn(UPDATED_AT, lit(updateTimestamp))
      .write
      .partitionBy(tablePartitions: _*)
      .mode(SaveMode.Append)
      .format("csv")
      .saveAsTable(fullTableName)
    spark.sql(s"msck repair table ${fullTableName}")
    logger.info(s"Table `${fullTableName}` is updated successfully.")
    snapshot
  }

  private def latestOid: Long =
    snapshot.select(OID).orderBy(col(OID).desc).as[Long].collect().headOption.getOrElse(Integer.MIN_VALUE)

  override def snapshot: Dataset[HighlightedSentence] = {
    spark.table(fullTableName).as[HighlightedSentence]
  }

}
