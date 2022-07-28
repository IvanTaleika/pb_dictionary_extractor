package pb.dictionary.extraction.stage

import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion}

import java.sql.Timestamp

/** Represents flatten PocketBook text highlight record. */
case class HighlightedText(
    oid: Long,
    text: String,
    title: String,
    authors: String,
    timeEdt: Long,
    updatedAt: Timestamp
) extends ApplicationManagedProduct

object HighlightedText extends ApplicationManagedProductCompanion[HighlightedText] {
  implicit val stageAreaDescriptor: this.type = this

  val OID             = "oid"
  val pk: Seq[String] = Seq(OID)

  val TITLE   = "title"
  val AUTHORS = "authors"

  /** Stores time when the text was highlighted */
  val TIME_EDT                      = "timeEdt"
  val copiedAttributes: Seq[String] = Seq(OID, TITLE, AUTHORS, TIME_EDT)

  val TEXT                               = "text"
  val transformedAttributes: Seq[String] = Seq(TEXT)
  val enrichedAttributes: Seq[String]    = Seq.empty

}
