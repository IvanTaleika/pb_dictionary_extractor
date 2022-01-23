package pb.dictionary.extraction.device

import java.sql.Timestamp

case class DeviceHighlight(
    oid: Long,
    `val`: String,
    title: String,
    authors: String,
    timeEdt: Long,
)

object DeviceHighlight {
  val OID      = "oid"
  val VAL      = "val"
  val TITLE    = "title"
  val AUTHORS  = "authors"
  val TIME_EDT = "timeEdt"

  case class HighlightInfo(
      begin: String,
      end: String,
      text: String,
      updated: Timestamp
  )

  object HighlightInfo {
    val BEGIN   = "begin"
    val END     = "end"
    val TEXT    = "text"
    val UPDATED = "updated"
  }
}
