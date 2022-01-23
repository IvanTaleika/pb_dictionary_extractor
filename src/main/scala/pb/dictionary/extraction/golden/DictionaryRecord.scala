package pb.dictionary.extraction.golden

import java.sql.Timestamp

case class DictionaryRecord(
    normalizedText: String,
    partOfSpeech: String,
    phonetic: String,
    forms: Seq[String],
    books: Seq[String],
    occurrences: Int,
    firstOccurrence: Timestamp,
    latestOccurrence: Timestamp,
    definition: String,
    example: String,
    synonyms: Seq[String],
    antonyms: Seq[String],
    translation: String,
    usage: Option[Double],
    updatedAt: Timestamp,
)

object DictionaryRecord {
  // silver
  val NORMALIZED_TEXT   = "normalizedText"
  val PART_OF_SPEECH    = "partOfSpeech"
  val PHONETIC          = "phonetic"
  val FORMS             = "forms"
  val BOOKS             = "books"
  val OCCURRENCES       = "occurrences"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  val DEFINITION        = "definition"
  val EXAMPLE           = "example"
  val SYNONYMS          = "synonyms"
  val ANTONYMS          = "antonyms"
  // new information
  val TRANSLATION = "translation"
  val USAGE       = "usage"

  // metadata
  val UPDATED_AT = "updatedAt"

  // TODO: Consider create a trait for PK, propagating attributes and enriched attributes
  val pk = Seq(
    NORMALIZED_TEXT,
    DEFINITION
  )

  val silverPropagatingCols = Seq(
    FORMS,
    BOOKS,
    OCCURRENCES,
    FIRST_OCCURRENCE,
    LATEST_OCCURRENCE,
    // It is safer to not include these attributes into PK, cause dictionary API can be extended to return richer responses
    PHONETIC,
    PART_OF_SPEECH,
    SYNONYMS,
    ANTONYMS,
    EXAMPLE,
  )

  val enrichedAttributes = Seq(
    TRANSLATION,
    USAGE
  )
}
