package pb.dictionary.extraction.golden

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait DictionaryTranslationApi {

  def translate(df: DataFrame): DataFrame
}

// TODO: implement
class DummyDictionaryTranslator extends DictionaryTranslationApi {
  import DictionaryRecord._
  override def translate(df: DataFrame): DataFrame =
    df.withColumn(TRANSLATION, concat(lit("dummy translation for "), col(NORMALIZED_TEXT)))
}
