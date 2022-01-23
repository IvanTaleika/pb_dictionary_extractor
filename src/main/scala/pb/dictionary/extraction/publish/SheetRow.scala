package pb.dictionary.extraction.publish

import java.sql.Timestamp

case class SheetRow(
    id: Int,
    // TODO: enum? Values - new / in progress / learned
    status: String,
    normalizedText: String,
    partOfSpeech: String,
    phonetic: String,
    forms: String,
    source: String,
    occurrences: Int,
    firstOccurrence: String,
    latestOccurrence: String,
    definition: String,
    examples: String,
    synonyms: String,
    antonyms: String,
    translation: String,
    usage: String,
    // CSV does not support timestamp - this is a partition column
    updatedAt: Timestamp,
)

object SheetRow {
  val ID                = "id"
  val STATUS            = "status"
  val NORMALIZED_TEXT   = "normalizedText"
  val PART_OF_SPEECH    = "partOfSpeech"
  val PHONETIC          = "phonetic"
  val FORMS             = "forms"
  val SOURCE            = "source"
  val OCCURRENCES       = "occurrences"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  val DEFINITION        = "definition"
  val EXAMPLES          = "examples"
  val SYNONYMS          = "synonyms"
  val ANTONYMS          = "antonyms"
  val TRANSLATION       = "translation"
  val USAGE             = "usage"
  val UPDATED_AT        = "updatedAt"
}
