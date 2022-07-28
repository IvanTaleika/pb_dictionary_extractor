package pb.dictionary.extraction.device

import pb.dictionary.extraction.ProductCompanion

import java.sql.Timestamp

/** A PocketBook database entry containing an information about a single mark on the book's page.
  * The mark can be a highlight, drawing, bookmark etc.
  */
case class PocketBookMark(
    oid: Long,
    `val`: String,
    title: String,
    authors: String,
    timeEdt: Long,
)

object PocketBookMark extends ProductCompanion[PocketBookMark] {
  implicit val deviceHighlightsDescriptor: this.type = this

  val OID = "oid"

  /** A JSON describing the mark. */
  val VAL      = "val"
  val TITLE    = "title"
  val AUTHORS  = "authors"
  val TIME_EDT = "timeEdt"

  /** A structure of a [[VAL]] JSON for a Highlight. */
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

  override val pk: Seq[String]         = Seq(OID)
  override val metadata: Seq[String]   = Seq.empty
}
