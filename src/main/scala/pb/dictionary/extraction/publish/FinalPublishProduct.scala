package pb.dictionary.extraction.publish

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructField
import pb.dictionary.extraction.{ApplicationManagedProduct, ApplicationManagedProductCompanion}

import scala.reflect.runtime.universe.TypeTag

trait FinalPublishProduct extends ApplicationManagedProduct {

  def id: Int
  def normalizedText: String
  def definition: String
  def forms: String
}

object FinalPublishProduct {
  val UPDATED_AT      = "updatedAt"
  val ID              = "id"
  val NORMALIZED_TEXT = "normalizedText"
  val DEFINITION      = "definition"
  val FORMS           = "forms"
}

abstract class FinalPublishProductProductCompanion[T <: ApplicationManagedProduct: TypeTag]
    extends ApplicationManagedProductCompanion[T] {

  final val ID              = FinalPublishProduct.ID
  final val NORMALIZED_TEXT = FinalPublishProduct.NORMALIZED_TEXT
  final val DEFINITION      = FinalPublishProduct.DEFINITION
  final val FORMS           = FinalPublishProduct.FORMS

  val pk: Seq[String] = Seq(ID)

  val naturalPk: Seq[String]            = Seq(NORMALIZED_TEXT, DEFINITION)
  val naturalPkCols: Seq[Column]        = naturalPk.map(col)
  val naturalPkFields: Seq[StructField] = naturalPk.map(colParameters)
}