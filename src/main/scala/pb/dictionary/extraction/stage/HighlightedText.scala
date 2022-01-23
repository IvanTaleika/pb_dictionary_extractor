package pb.dictionary.extraction.stage

import java.sql.Timestamp

case class HighlightedText(
    oid: Long,
    text: String,
    title: String,
    authors: String,
    timeEdt: Long,
    updatedAt: Timestamp
)

object HighlightedText {
  val OID      = "oid"
  val TEXT     = "text"
  val TITLE    = "title"
  val AUTHORS  = "authors"
  val TIME_EDT = "timeEdt"
  val UPDATED_AT = "updatedAt"
}