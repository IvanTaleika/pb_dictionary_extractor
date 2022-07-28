package pb.dictionary.extraction

import org.apache.spark.sql.{Column, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import pb.dictionary.extraction.sql.functions._

import scala.reflect.runtime.universe.TypeTag

/** An utility trait for [[Product]] companion objects */
abstract class ProductCompanion[T <: Product: TypeTag] {

  def schema: StructType          = Encoders.product[T].schema
  def attributes: Seq[String]     = schema.map(_.name)
  def attributesCols: Seq[Column] = attributes.map(col)

  def pk: Seq[String]
  def pkCols(colFun: String => Column = col): Seq[Column] = pk.map(colFun)
  def pkFields: Seq[StructField] = pk.map(colParameters)

  def nonPk: Seq[String] = attributes.filter(pk.toSet.contains)
  def nonPkCols(colFun: String => Column = col): Seq[Column] = nonPk.map(colFun)
  def nonPkFields: Seq[StructField] = nonPk.map(colParameters)

  def metadata: Seq[String]
  def metadataCols(colFun: String => Column = col): Seq[Column] = metadata.map(colFun)
  def metadataFields: Seq[StructField] = metadata.map(colParameters)

  def businessNonPk: Seq[String] = nonPk.filter(metadata.toSet.contains)
  def businessNonPkCols(colFun: String => Column = col): Seq[Column] = businessNonPk.map(colFun)
  def businessNonPkFields: Seq[StructField] = businessNonPk.map(colParameters)

  def pkMatches(colFun1: String => Column, colFun2: String => Column) =
    pk.map(cn => colFun1(cn) === colFun2(cn)).foldLeft(lit(true))(_ && _)

  def pkMatches(t1: String, t2: String) = pkMatches(colFromTable(t1) _, colFromTable(t2) _)

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
