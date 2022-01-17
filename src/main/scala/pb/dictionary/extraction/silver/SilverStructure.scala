package pb.dictionary.extraction.silver

import java.sql.Timestamp

case class DefinedWord(
    text: String,
    books: Seq[String],
    occurrences: Int,
    firstOccurrence: Timestamp,
    latestOccurrence: Timestamp,
    updatedAt: Timestamp,
    normalizedText: String,
    phonetic: String,
    partOfSpeech: String,
    definition: String,
    example: String,
    synonyms: Seq[String],
    antonyms: Seq[String]
)

object DefinedWord {

  // bronze
  val TEXT              = "text"
  val BOOKS             = "books"
  val OCCURRENCES       = "occurrences"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  // new information
  val NORMALIZED_TEXT = "normalizedText"
  val PHONETIC        = "phonetic"
  val PART_OF_SPEECH  = "partOfSpeech"
  val DEFINITION      = "definition"
  val EXAMPLE         = "example"
  val SYNONYMS        = "synonyms"
  val ANTONYMS        = "antonyms"
  // metadata
  val UPDATED_AT = "updatedAt"

  val pk = Seq(TEXT)

  val bronzePropagatingCols = Seq(
    BOOKS,
    OCCURRENCES,
    FIRST_OCCURRENCE,
    LATEST_OCCURRENCE
  )

  val definitionCols = Seq(
    NORMALIZED_TEXT,
    PHONETIC,
    PART_OF_SPEECH,
    DEFINITION,
    SYNONYMS,
    ANTONYMS
  )
}
