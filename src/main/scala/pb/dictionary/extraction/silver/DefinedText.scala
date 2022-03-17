package pb.dictionary.extraction.silver

import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion}

import java.sql.Timestamp

case class DefinedText(
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
    examples: Seq[String],
    synonyms: Seq[String],
    antonyms: Seq[String]
) extends ApplicationManagedProduct

object DefinedText extends ApplicationManagedProductCompanion[DefinedText] {
  implicit val silverAreaDescriptor: this.type = this

  val TEXT = "text"
  val pk   = Seq(TEXT)

  val BOOKS             = "books"
  val OCCURRENCES       = "occurrences"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  val propagatingAttributes: Seq[String] = Seq(
    BOOKS,
    OCCURRENCES,
    FIRST_OCCURRENCE,
    LATEST_OCCURRENCE
  )

  val NORMALIZED_TEXT = "normalizedText"
  val PHONETIC        = "phonetic"
  val PART_OF_SPEECH  = "partOfSpeech"
  val DEFINITION      = "definition"
  val EXAMPLES        = "examples"
  val SYNONYMS        = "synonyms"
  val ANTONYMS        = "antonyms"
  val enrichedAttributes: Seq[String] = Seq(
    NORMALIZED_TEXT,
    PHONETIC,
    PART_OF_SPEECH,
    DEFINITION,
    EXAMPLES,
    SYNONYMS,
    ANTONYMS
  )

}
