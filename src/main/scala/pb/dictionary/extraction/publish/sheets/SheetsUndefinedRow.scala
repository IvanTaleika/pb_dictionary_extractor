package pb.dictionary.extraction.publish.sheets

import pb.dictionary.extraction.ProductCompanion

/** Represents a PocketBook text highlight token that wasn't enriched by the definition automatically. */
case class SheetsUndefinedRow(
    text: String,
    books: String,
    occurrences: Int,
    firstOccurrence: String,
    latestOccurrence: String
)

object SheetsUndefinedRow extends ProductCompanion[SheetsUndefinedRow] {
  implicit val manualEnrichmentAreaDescriptor: this.type = this

  val TEXT            = "text"
  val pk: Seq[String] = Seq(TEXT)

  val OCCURRENCES       = "occurrences"
  val BOOKS             = "books"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"

  override val metadata: Seq[String]   = Seq.empty
}
