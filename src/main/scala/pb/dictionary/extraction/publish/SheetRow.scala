package pb.dictionary.extraction.publish

import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion}

import java.sql.Timestamp

case class SheetRow(
    id: Int,
    status: String, // new / in progress / learned
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
) extends ApplicationManagedProduct

object SheetRow extends ApplicationManagedProductCompanion[SheetRow] {
  val ID              = "id"
  val pk: Seq[String] = Seq(ID)

  val NORMALIZED_TEXT = "normalizedText"
  val PART_OF_SPEECH  = "partOfSpeech"
  val PHONETIC        = "phonetic"
  val FORMS           = "forms"
  val OCCURRENCES     = "occurrences"
  val DEFINITION      = "definition"
  val TRANSLATION     = "translation"
  val propagatingAttributes: Seq[String] =
    Seq(NORMALIZED_TEXT, PART_OF_SPEECH, PHONETIC, FORMS, OCCURRENCES, DEFINITION, TRANSLATION)

  val STATUS            = "status"
  val SOURCE            = "source"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  val EXAMPLES          = "examples"
  val SYNONYMS          = "synonyms"
  val ANTONYMS          = "antonyms"
  val USAGE             = "usage"
  val enrichedAttributes: Seq[String] =
    Seq(STATUS, SOURCE, FIRST_OCCURRENCE, LATEST_OCCURRENCE, EXAMPLES, SYNONYMS, ANTONYMS, USAGE)

  val NewStatus        = "new"
  val InProgressStatus = "in progress"
  val LearnedStatus    = "learned"

}
