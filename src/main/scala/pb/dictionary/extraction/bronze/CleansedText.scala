package pb.dictionary.extraction.bronze

import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion}

import java.sql.Timestamp

case class CleansedText(
    text: String,
    books: Seq[String],
    occurrences: Int,
    firstOccurrence: Timestamp,
    latestOccurrence: Timestamp,
    updatedAt: Timestamp
) extends ApplicationManagedProduct

object CleansedText extends ApplicationManagedProductCompanion[CleansedText] {
  implicit val bronzeAreaDescriptor: this.type = this

  val TEXT            = "text"
  val pk: Seq[String] = Seq(TEXT)

  val propagatingAttributes: Seq[String] = Seq.empty

  val BOOKS                           = "books"
  val OCCURRENCES                     = "occurrences"
  val FIRST_OCCURRENCE                = "firstOccurrence"
  val LATEST_OCCURRENCE               = "latestOccurrence"
  val enrichedAttributes: Seq[String] = Seq(BOOKS, OCCURRENCES, FIRST_OCCURRENCE, LATEST_OCCURRENCE)
}
