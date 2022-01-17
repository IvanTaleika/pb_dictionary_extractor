package pb.dictionary.extraction.stage

import java.sql.Timestamp

case class HighlightedSentence(
    oid: Long,
    text: String,
    title: String,
    authors: String,
    timeEdt: Long,
)

object HighlightedSentence {
  val OID      = "oid"
  val TEXT     = "text"
  val TITLE    = "title"
  val AUTHORS  = "authors"
  val TIME_EDT = "timeEdt"
}

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
