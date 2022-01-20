package pb.dictionary.extraction.publish

case class UndefinedText(
    text: String,
    books: String,
    occurrences: Int,
    firstOccurrence: String,
    latestOccurrence: String,
    updatedAt: String
)

object UndefinedText {
  val TEXT              = "text"
  val BOOKS             = "books"
  val OCCURRENCES       = "occurrences"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  val UPDATED_AT        = "updatedAt"
}
