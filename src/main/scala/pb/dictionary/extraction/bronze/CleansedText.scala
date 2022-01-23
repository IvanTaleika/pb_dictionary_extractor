package pb.dictionary.extraction.bronze

import java.sql.Timestamp

case class CleansedText(
    text: String,
    books: Seq[String],
    occurrences: Int,
    firstOccurrence: Timestamp,
    latestOccurrence: Timestamp,
    updatedAt: Timestamp
)

object CleansedText {
  val TEXT              = "text"
  val BOOKS             = "books"
  val OCCURRENCES       = "occurrences"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  val UPDATED_AT        = "updatedAt"
}
