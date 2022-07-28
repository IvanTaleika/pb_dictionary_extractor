package pb.dictionary.extraction.golden

import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion}

import java.sql.Timestamp

// TODO: doc
case class VocabularyRecord(
    normalizedText: String,
    partOfSpeech: String,
    phonetic: String,
    forms: Seq[String],
    books: Seq[String],
    occurrences: Int,
    firstOccurrence: Timestamp,
    latestOccurrence: Timestamp,
    definition: String,
    examples: Seq[String],
    synonyms: Seq[String],
    antonyms: Seq[String],
    translations: Seq[String],
    usage: Option[Double],
    updatedAt: Timestamp,
) extends ApplicationManagedProduct

object VocabularyRecord extends ApplicationManagedProductCompanion[VocabularyRecord] {
  implicit val goldenAreaDescriptor: this.type = this

  val NORMALIZED_TEXT = "normalizedText"
  val DEFINITION      = "definition"
  val pk              = Seq(NORMALIZED_TEXT, DEFINITION)

  val copiedAttributes: Seq[String] = pk

  val PART_OF_SPEECH    = "partOfSpeech"
  val PHONETIC          = "phonetic"
  val FORMS             = "forms"
  val BOOKS             = "books"
  val OCCURRENCES       = "occurrences"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  val EXAMPLES          = "examples"
  val SYNONYMS          = "synonyms"
  val ANTONYMS          = "antonyms"

  val transformedAttributes: Seq[String] = Seq(
    FORMS,
    BOOKS,
    OCCURRENCES,
    FIRST_OCCURRENCE,
    LATEST_OCCURRENCE,
    PHONETIC,
    PART_OF_SPEECH,
    SYNONYMS,
    ANTONYMS,
    EXAMPLES
  )

  /** Populated by [[DictionaryTranslationApi]] */
  val TRANSLATIONS = "translations"

  /** Populated by [[UsageFrequencyApi]] */
  val USAGE = "usage"

  val enrichedAttributes = Seq(TRANSLATIONS, USAGE)
}
