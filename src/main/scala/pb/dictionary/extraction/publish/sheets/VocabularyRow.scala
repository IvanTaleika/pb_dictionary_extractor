package pb.dictionary.extraction.publish.sheets

import pb.dictionary.extraction.publish.{FinalPublishProduct, FinalPublishProductCompanion}

case class VocabularyRow(
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
    translations: String,
    usage: String,
    tags: String,
    notes: String,
) extends FinalPublishProduct

object VocabularyRow extends FinalPublishProductCompanion[VocabularyRow] {
  implicit val googleSheetsAreaDescriptor: this.type = this

  val PART_OF_SPEECH    = "partOfSpeech"
  val PHONETIC          = "phonetic"
  val OCCURRENCES       = "occurrences"
  val TRANSLATIONS      = "translations"
  val STATUS            = "status"
  val SOURCES           = "sources"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  val EXAMPLES          = "examples"
  val SYNONYMS          = "synonyms"
  val ANTONYMS          = "antonyms"
  val USAGE             = "usage"
  val TAGS              = "tags"
  val NOTES             = "notes"

  val attributes: Seq[String] =
    Seq(
      NORMALIZED_TEXT,
      PART_OF_SPEECH,
      PHONETIC,
      FORMS,
      OCCURRENCES,
      DEFINITION,
      TRANSLATIONS,
      STATUS,
      SOURCES,
      FIRST_OCCURRENCE,
      LATEST_OCCURRENCE,
      EXAMPLES,
      SYNONYMS,
      ANTONYMS,
      USAGE,
      TAGS,
      NOTES
    )

  val metadata: Seq[String] = Seq.empty

  val NewStatus        = "new"
  val InProgressStatus = "in progress"
  val LearnedStatus    = "learned"
  val UsageDecimals    = 6
}
