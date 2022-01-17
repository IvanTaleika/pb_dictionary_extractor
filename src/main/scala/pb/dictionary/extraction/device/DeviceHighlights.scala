package pb.dictionary.extraction.device

import org.apache.spark.sql.Dataset
import pb.dictionary.extraction.Area

class DeviceHighlights(val path: String) extends Area[DeviceHighlight] {
  private val HighlightTagId = 104
  import DeviceHighlight._

  override def snapshot: Dataset[DeviceHighlight] = {
    import spark.implicits._
    spark.read
      .format("jdbc")
      .option("driver", "org.sqlite.JDBC")
      .option("url", s"jdbc:sqlite:$path")
      .option(
        "query",
        s"""
           |select
           |  t.$OID,
           |  t.$VAL,
           |  b.$TITLE,
           |  b.$AUTHORS,
           |  t.$TIME_EDT
           |from Tags as t
           |join items as i
           |  on t.ItemID = i.OID
           |join items as ii
           |  on i.ParentID = ii.OID
           |join books as b
           |  on ii.OID = b.OID
           |where t.TagID = $HighlightTagId""".stripMargin
      )
      .load()
      .as[DeviceHighlight]
  }
}
