package pb.dictionary.extraction.publish

import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion}

import java.sql.Timestamp

case class UndefinedText(
    text: String,
    books: String,
    occurrences: Int,
    firstOccurrence: String,
    latestOccurrence: String,
    updatedAt: Timestamp
) extends ApplicationManagedProduct

object UndefinedText extends ApplicationManagedProductCompanion[ApplicationManagedProduct] {
  val TEXT            = "text"
  val pk: Seq[String] = Seq(TEXT)

  val OCCURRENCES                        = "occurrences"
  val propagatingAttributes: Seq[String] = Seq(OCCURRENCES)

  val BOOKS                           = "books"
  val FIRST_OCCURRENCE                = "firstOccurrence"
  val LATEST_OCCURRENCE               = "latestOccurrence"
  val enrichedAttributes: Seq[String] = Seq(BOOKS, FIRST_OCCURRENCE, LATEST_OCCURRENCE)

}
