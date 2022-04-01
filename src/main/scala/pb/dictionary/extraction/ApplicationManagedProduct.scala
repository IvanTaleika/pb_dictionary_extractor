package pb.dictionary.extraction

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructField

import java.sql.Timestamp

/** [[Product]] api extension to enforce [[ApplicationManagedArea]] to specify record update timestamp in
  * [[ApplicationManagedProductCompanion.UPDATED_AT]] column for traceability and filtering.
  */
trait ApplicationManagedProduct extends Product {

  def updatedAt: Timestamp
}

object ApplicationManagedProduct {
  val UPDATED_AT = "updatedAt"
}

trait ApplicationManagedProductCompanion[T <: ApplicationManagedProduct] extends ProductCompanion[T] {

  def propagatingAttributes: Seq[String]
  def enrichedAttributes: Seq[String]

  final val UPDATED_AT = ApplicationManagedProduct.UPDATED_AT

  def metadata: Seq[String]   = Seq(UPDATED_AT)
  def attributes: Seq[String] = propagatingAttributes ++ enrichedAttributes

  def propagatingAttributesCols: Seq[Column] = propagatingAttributes.map(col)
  def enrichedAttributesCols: Seq[Column]    = enrichedAttributes.map(col)

  def propagatingAttributesFields: Seq[StructField] = propagatingAttributes.map(colParameters)
  def enrichedAttributesFields: Seq[StructField]    = enrichedAttributes.map(colParameters)
}
