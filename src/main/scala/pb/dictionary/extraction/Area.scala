package pb.dictionary.extraction

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag

abstract class Area[Out <: Product: TypeTag] {
  protected val spark    = SparkSession.active
  val schema: StructType = Encoders.product[Out].schema

  def path: String
  def snapshot: Dataset[Out]
}