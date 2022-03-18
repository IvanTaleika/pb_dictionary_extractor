package pb.dictionary.extraction.publish.sheets

import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion, ProductCompanion}

import java.sql.Timestamp

case class UndefinedRow(
    text: String,
    books: String,
    occurrences: Int,
    firstOccurrence: String,
    latestOccurrence: String
)

object UndefinedRow extends ProductCompanion[UndefinedRow] {
  implicit val manualEnrichmentAreaDescriptor: this.type = this

  val TEXT            = "text"
  val pk: Seq[String] = Seq(TEXT)

  val OCCURRENCES       = "occurrences"
  val BOOKS             = "books"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"

  override val attributes: Seq[String] = Seq(OCCURRENCES, BOOKS, FIRST_OCCURRENCE, LATEST_OCCURRENCE)
  override val metadata: Seq[String]   = Seq.empty
}
