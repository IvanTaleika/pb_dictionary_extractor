package pb.dictionary.extraction.silver

import org.apache.spark.sql.{DataFrame, Dataset}
import pb.dictionary.extraction.bronze.CleansedText

/** Enriches [[DataFrame]] with dictionary definition and normalized text form.
  */
trait WordDefinitionApi {

  /**
    * Enriches [[DataFrame]] with [[DefinedText.DEFINITION]], [[DefinedText.NORMALIZED_TEXT]]
    * and possibly others, supplemental, columns.
    */
  def define(df: Dataset[CleansedText]): DataFrame
}
