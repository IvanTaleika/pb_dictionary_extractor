package pb.dictionary.extraction.device

import org.apache.spark.sql.Dataset
import pb.dictionary.extraction.Area

/** Stores all the marks left on the PocketBook books' pages.
  * The area is backed Sqlite BD and managed by the by PocketBook device
  */
class PocketBookMarksArea(val path: String) extends Area[PocketBookMark] {
  private val MarkTagId = 104
  import PocketBookMark._

  override def snapshot: Dataset[PocketBookMark] = {
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
           |where t.TagID = $MarkTagId""".stripMargin
      )
      .load()
      .as[PocketBookMark]
  }
}
