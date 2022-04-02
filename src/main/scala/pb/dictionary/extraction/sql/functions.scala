package pb.dictionary.extraction.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object functions {
  def colFromTable(tableAlias: String)(cn: String): Column = col(s"$tableAlias.$cn")

}
