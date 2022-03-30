package pb.dictionary.extraction.golden

import org.apache.spark.sql.DataFrame

trait DictionaryTranslationApi {

  def translate(df: DataFrame): DataFrame
}