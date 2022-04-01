package pb.dictionary.extraction.golden

import org.apache.spark.sql.DataFrame

/** Enriches [[DataFrame]] with dictionary translations. That means text can receive multiple
  * translations if it has multiple definitions.
  */
trait DictionaryTranslationApi {

  /** Enriches [[DataFrame]] with [[RichDefinedText.TRANSLATIONS]] column. */
  def translate(df: DataFrame): DataFrame
}
