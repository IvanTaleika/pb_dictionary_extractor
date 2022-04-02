package pb.dictionary.extraction

import org.apache.spark.sql.{Column, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import pb.dictionary.extraction.sql.functions._

import scala.reflect.runtime.universe.TypeTag

/** An utility trait for [[Product]] companion objects */
abstract class ProductCompanion[T <: Product: TypeTag] {

  def schema: StructType = Encoders.product[T].schema

  def pk: Seq[String]
  def attributes: Seq[String]
  def metadata: Seq[String]

  def pkCols: Seq[Column]         = pk.map(col)
  def attributesCols: Seq[Column] = attributes.map(col)
  def metadataCols: Seq[Column]   = metadata.map(col)

  def pkFields: Seq[StructField]         = pk.map(colParameters)
  def attributesFields: Seq[StructField] = attributes.map(colParameters)
  def metadataFields: Seq[StructField]   = metadata.map(colParameters)

  def pkMatches(t1: String, t2: String) =
    pk.map(cn => colFromTable(t1)(cn) === colFromTable(t2)(cn)).foldLeft(lit(true))(_ && _)

  protected lazy val colParameters = {
    val searchSchema = schema.map(dt => dt.name -> dt).toMap
    (cn: String) =>
      searchSchema(cn)
  }

  /** import allows syntax such as:
    *
    * {{{
    *   override def snapshot: Dataset[Out] = {
    *     import areaDescriptor.implicits._
    *     import spark.implicits._
    *     spark.table(fullTableName).as[Out]
    *   }
    * }}}
    */
  object implicits {
    // Adding type to this value fails the build ¯\_(ツ)_/¯
    implicit val areaTypeTag = implicitly[TypeTag[T]]
  }
}
