package pb.dictionary.extraction.silver

import org.apache.spark.sql.Dataset
import pb.dictionary.extraction.bronze.CleansedWord

trait WordDefinitionApi {

  def define(df: Dataset[CleansedWord]): Dataset[DefinedWord]
}
