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
    sources: String,
    occurrences: Int,
    firstOccurrence: String,
    latestOccurrence: String,
    definition: String,
    examples: String,
    synonyms: String,
    antonyms: String,
    translation: String,
    usage: String,
    tags: String,
    notes: String,
    updatedAt: Timestamp,
) extends FinalPublishProduct

object SheetRow extends FinalPublishProductProductCompanion[SheetRow] {

  val PART_OF_SPEECH = "partOfSpeech"
  val PHONETIC       = "phonetic"
  val OCCURRENCES    = "occurrences"
  val TRANSLATION    = "translation"
  val propagatingAttributes: Seq[String] =
    Seq(NORMALIZED_TEXT, PART_OF_SPEECH, PHONETIC, FORMS, OCCURRENCES, DEFINITION, TRANSLATION)

  val STATUS            = "status"
  val SOURCES           = "sources"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  val EXAMPLES          = "examples"
  val SYNONYMS          = "synonyms"
  val ANTONYMS          = "antonyms"
  val USAGE             = "usage"
  val enrichedAttributes: Seq[String] =
    Seq(STATUS, SOURCES, FIRST_OCCURRENCE, LATEST_OCCURRENCE, EXAMPLES, SYNONYMS, ANTONYMS, USAGE)

  val attributesOrder = Seq(
    ID,
    STATUS,
    NORMALIZED_TEXT,
    PART_OF_SPEECH,
    PHONETIC,
    FORMS,
    SOURCES,
    OCCURRENCES,
    FIRST_OCCURRENCE,
    LATEST_OCCURRENCE,
    DEFINITION,
    EXAMPLES,
    SYNONYMS,
    ANTONYMS,
    TRANSLATION,
    USAGE
  )

  val NewStatus        = "new"
  val InProgressStatus = "in progress"
  val LearnedStatus    = "learned"
  val UsageDecimals    = 6

}
