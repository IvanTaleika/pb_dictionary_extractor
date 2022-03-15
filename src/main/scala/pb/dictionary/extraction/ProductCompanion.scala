package pb.dictionary.extraction

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Encoders}
import org.apache.spark.sql.types.{StructField, StructType}
import scala.reflect.runtime.universe.TypeTag

abstract class ProductCompanion[T <: Product: TypeTag] {

  def schema: StructType = Encoders.product[T].schema

  def pk: Seq[String]
  def propagatingAttributes: Seq[String]
  def enrichedAttributes: Seq[String]
  def metadata: Seq[String]

  def pkCols: Seq[Column]                    = pk.map(col)
  def propagatingAttributesCols: Seq[Column] = propagatingAttributes.map(col)
  def enrichedAttributesCols: Seq[Column]    = enrichedAttributes.map(col)
  def metadataCols: Seq[Column]              = metadata.map(col)

  def pkFields: Seq[StructField]                    = pk.map(colParameters)
  def propagatingAttributesFields: Seq[StructField] = propagatingAttributes.map(colParameters)
  def enrichedAttributesFields: Seq[StructField]    = enrichedAttributes.map(colParameters)
  def metadataFields: Seq[StructField]              = metadata.map(colParameters)

  protected lazy val colParameters = {
    val searchSchema = schema.map(dt => dt.name -> dt).toMap
    (cn: String) =>
      searchSchema(cn)
  }
}