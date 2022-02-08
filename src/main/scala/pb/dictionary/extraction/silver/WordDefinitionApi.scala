package pb.dictionary.extraction.silver

import org.apache.spark.sql.{DataFrame, Dataset}
import pb.dictionary.extraction.bronze.CleansedText

trait WordDefinitionApi {

  def define(df: Dataset[CleansedText]): DataFrame
}
