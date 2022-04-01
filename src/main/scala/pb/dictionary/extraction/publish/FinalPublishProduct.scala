package pb.dictionary.extraction.publish

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructField
import pb.dictionary.extraction.ProductCompanion

/**
  * Specifies a minimal requirements for a end product record:
  *
  *   - [[FinalPublishProduct.ID]] a synthetic ID column
  *   - [[FinalPublishProduct.NORMALIZED_TEXT]] a normalized text to learn
  *   - [[FinalPublishProduct.DEFINITION]] a normalized text definition
  *   - [[FinalPublishProduct.FORMS]] text, highlighted on the PocketBook page
  */
trait FinalPublishProduct extends Product {

  def id: Int
  def normalizedText: String
  def definition: String
  def forms: String
}

object FinalPublishProduct {
  val ID              = "id"
  val NORMALIZED_TEXT = "normalizedText"
  val DEFINITION      = "definition"
  val FORMS           = "forms"
}

trait FinalPublishProductCompanion[T <: FinalPublishProduct] extends ProductCompanion[T] {

  final val ID              = FinalPublishProduct.ID
  final val NORMALIZED_TEXT = FinalPublishProduct.NORMALIZED_TEXT
  final val DEFINITION      = FinalPublishProduct.DEFINITION
  final val FORMS           = FinalPublishProduct.FORMS

  val pk: Seq[String] = Seq(ID)

  val naturalPk: Seq[String]            = Seq(NORMALIZED_TEXT, DEFINITION)
  val naturalPkCols: Seq[Column]        = naturalPk.map(col)
  val naturalPkFields: Seq[StructField] = naturalPk.map(colParameters)
}
