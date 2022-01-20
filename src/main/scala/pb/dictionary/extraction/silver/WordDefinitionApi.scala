package pb.dictionary.extraction.silver

import org.apache.spark.sql.Dataset
import pb.dictionary.extraction.bronze.CleansedText

trait WordDefinitionApi {

  def define(df: Dataset[CleansedText]): Dataset[DefinedText]
}
