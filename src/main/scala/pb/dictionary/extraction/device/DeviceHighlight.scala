package pb.dictionary.extraction.device

import pb.dictionary.extraction.ProductCompanion

import java.sql.Timestamp

case class DeviceHighlight(
    oid: Long,
    `val`: String,
    title: String,
    authors: String,
    timeEdt: Long,
)

object DeviceHighlight extends ProductCompanion[DeviceHighlight] {
  implicit val deviceHighlightsDescriptor: this.type = this

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

  override val pk: Seq[String] = Seq(OID)
  override val attributes: Seq[String] = Seq(VAL, TITLE, AUTHORS, TIME_EDT)
  override val metadata: Seq[String] = Seq.empty
}
