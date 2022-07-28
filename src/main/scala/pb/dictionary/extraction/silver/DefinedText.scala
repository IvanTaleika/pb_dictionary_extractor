package pb.dictionary.extraction.silver

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion}

import java.sql.Timestamp

/** Represents a minimal vocabulary record ([[normalizedText]] + [[definition]]) enriched with
  * non-essential PocketBook and [[TextDefinitionApi]] attributes.
  */
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

  val TEXT            = "text"
  val NORMALIZED_TEXT = "normalizedText"
  val DEFINITION      = "definition"

  val pk = Seq(TEXT, NORMALIZED_TEXT, DEFINITION)

  val BOOKS             = "books"
  val OCCURRENCES       = "occurrences"
  val FIRST_OCCURRENCE  = "firstOccurrence"
  val LATEST_OCCURRENCE = "latestOccurrence"
  val copiedAttributes: Seq[String] = Seq(
    TEXT,
    BOOKS,
    OCCURRENCES,
    FIRST_OCCURRENCE,
    LATEST_OCCURRENCE
  )

  val transformedAttributes: Seq[String] = Seq.empty

  val PHONETIC       = "phonetic"
  val PART_OF_SPEECH = "partOfSpeech"
  val EXAMPLES       = "examples"
  val SYNONYMS       = "synonyms"
  val ANTONYMS       = "antonyms"
  val enrichedAttributes: Seq[String] = Seq(
    NORMALIZED_TEXT,
    DEFINITION,
    PHONETIC,
    PART_OF_SPEECH,
    EXAMPLES,
    SYNONYMS,
    ANTONYMS
  )

  def isDefined(colFunc: String => Column = col): Column =
    colFunc(NORMALIZED_TEXT) =!= SilverArea.definitionNotFoundPlaceholder
  def nonDefined(colFunc: String => Column = col): Column = !isDefined(colFunc)
}

// TODO: Where does this list comes from? Compare parts of speech from the dictionary with this mapping
/** All possible [[DefinedText.PART_OF_SPEECH]] values, produced by [[DictionaryApiDevTextDefiner]]. */
object PartOfSpeech {
  val NOUN              = "noun"
  val VERB              = "verb"
  val ADJECTIVE         = "adjective"
  val ADVERB            = "adverb"
  val PRONOUN           = "pronoun"
  val DETERMINER        = "determiner"
  val PREPOSITION       = "preposition"
  val NUMBER            = "number"
  val CONJUNCTION       = "conjunction"
  val PARTICLE          = "particle"
  val INFINITIVE_MARKER = "infinitive marker"
  val EXCLAMATION       = "exclamation"
  // this parts of speech were unseen in the requests, but we keep them just in case
  val POSTPOSITION = "postposition"
  val ARTICLE      = "article"
}
