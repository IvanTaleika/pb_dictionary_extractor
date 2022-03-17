package pb.dictionary.extraction.stage

import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion}

import java.sql.Timestamp

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

  val TEXT                            = "text"
  val enrichedAttributes: Seq[String] = Seq(TEXT)

  val TITLE                              = "title"
  val AUTHORS                            = "authors"
  val TIME_EDT                           = "timeEdt"
  val propagatingAttributes: Seq[String] = Seq(TITLE, AUTHORS, TIME_EDT)

}
