package pb.dictionary.extraction.golden

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait DictionaryTranslationApi {

  // TODO: typesafe?
  def translate(df: DataFrame): DataFrame
}

class DummyDictionaryTranslator extends DictionaryTranslationApi {
  import DictionaryRecord._
  override def translate(df: DataFrame): DataFrame =
    df.withColumn(TRANSLATION, concat(lit("dummy translation for "), col(NORMALIZED_TEXT)))
}
