package pb.dictionary.extraction.golden

import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion}

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
    examples: Seq[String],
    synonyms: Seq[String],
    antonyms: Seq[String],
    translation: String,
    usage: Option[Double],
    updatedAt: Timestamp,
) extends ApplicationManagedProduct

object DictionaryRecord extends ApplicationManagedProductCompanion[DictionaryRecord] {
  implicit val goldenAreaDescriptor: this.type = this

  // silver
  val NORMALIZED_TEXT = "normalizedText"
  val DEFINITION      = "definition"
  val pk              = Seq(NORMALIZED_TEXT, DEFINITION)

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
  val propagatingAttributes: Seq[String] = Seq(
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
    EXAMPLES,
  )

  val TRANSLATION        = "translation"
  val USAGE              = "usage"
  val enrichedAttributes = Seq(TRANSLATION, USAGE)

}
