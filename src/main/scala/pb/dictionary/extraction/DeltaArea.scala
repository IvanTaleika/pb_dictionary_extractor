package pb.dictionary.extraction

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag

abstract class DeltaArea[In, Out <: Product: TypeTag](val path: String)
    extends ApplicationManagedArea[In, Out](path, "delta") {
  import spark.implicits._
  protected def deltaTable                                   = DeltaTable.forPath(absoluteTablePath)
  override def snapshot: Dataset[Out]                        = deltaTable.toDF.as[Out]
  protected def stagingAlias                                 = "staging"
  protected def colFromTable(tableAlias: String)(cn: String) = col(s"$tableAlias.$cn")
  protected def colDelta(cn: String)                         = colFromTable(tableName)(cn)
  protected def colStaged(cn: String)                        = colFromTable(stagingAlias)(cn)
}