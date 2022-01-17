package pb.dictionary.extraction.stage

import org.apache.spark.sql.{Dataset, Encoders, SaveMode}
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.ApplicationManagedArea
import pb.dictionary.extraction.device.DeviceHighlight

class StageArea(val path: String) extends ApplicationManagedArea[DeviceHighlight, HighlightedSentence](path, "csv") {
  import HighlightedSentence._
  import spark.implicits._
  override protected def tableOptions = Map("multiline" -> "true", "header" -> "true", "mode" -> "FAILFAST")

  override def upsert(updates: Dataset[DeviceHighlight], snapshot: Dataset[DeviceHighlight]): Dataset[HighlightedSentence] = {
    updates.transform(fromUserHighlights).transform(writeNew)
  }

  private def fromUserHighlights(userHighlights: Dataset[DeviceHighlight]): Dataset[HighlightedSentence] = {
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
      .as[HighlightedSentence]
  }

  private def writeNew(wordHighlights: Dataset[HighlightedSentence]): Dataset[HighlightedSentence] = {
    // TODO: check filter pushdowns when working with DB
    val newWordHighlights = wordHighlights.where(col(OID) > lit(latestOid))
    newWordHighlights.write
      .mode(SaveMode.Append)
      .format("csv")
      .saveAsTable(fullTableName)
    newWordHighlights
  }

  private def latestOid: Long =
    snapshot.select(OID).orderBy(col(OID).desc).as[Long].collect().headOption.getOrElse(Integer.MIN_VALUE)

  override def snapshot: Dataset[HighlightedSentence] = {
    spark.table(fullTableName).as[HighlightedSentence]
  }

}
