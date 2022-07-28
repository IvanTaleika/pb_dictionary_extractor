package pb.dictionary.extraction.publish.sheets

import pb.dictionary.extraction.publish.{FinalPublishProduct, FinalPublishProductCompanion}

/** Represents a Google sheet row that correspond to a single vocabulary entry. */
case class SheetsVocabularyRow(
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

object SheetsVocabularyRow extends FinalPublishProductCompanion[SheetsVocabularyRow] {
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

  val metadata: Seq[String] = Seq.empty

  val NewStatus        = "new"
  val InProgressStatus = "in progress"
  val LearnedStatus    = "learned"
  val UsageDecimals    = 6
}
