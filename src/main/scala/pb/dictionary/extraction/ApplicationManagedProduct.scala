package pb.dictionary.extraction

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import pb.dictionary.extraction.enrichment.EnrichmentDefaults

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
  final val UPDATED_AT      = ApplicationManagedProduct.UPDATED_AT
  def metadata: Seq[String] = Seq(UPDATED_AT)

  def copiedAttributes: Seq[String]
  def copiedAttributesCols(colFun: String => Column = col): Seq[Column] = copiedAttributes.map(colFun)
  def copiedAttributesFields: Seq[StructField] = copiedAttributes.map(colParameters)

  def transformedAttributes: Seq[String]
  def transformedAttributesCols(colFun: String => Column = col): Seq[Column] = transformedAttributes.map(colFun)
  def transformedAttributesFields: Seq[StructField] = transformedAttributes.map(colParameters)

  def enrichedAttributes: Seq[String]
  def enrichedAttributesCols(colFun: String => Column = col): Seq[Column] = enrichedAttributes.map(colFun)
  def enrichedAttributesFields: Seq[StructField] = enrichedAttributes.map(colParameters)

  // TODO: variable names
  trait DefaultEnrichment {
    def value: Any
    def isDefinedFor(dt: DataType): Boolean

    def pf = new PartialFunction[DataType, Any] {
      override def isDefinedAt(x: DataType): Boolean = isDefinedFor(x)

      override def apply(v1: DataType): Any = value
    }
  }

  case class StringDefaultEnrichment(value: String) extends DefaultEnrichment {
    override def isDefinedFor(dt: DataType): Boolean = dt == StringType
  }

  case class StringSeqDefaultEnrichment(value: Seq[String]) extends DefaultEnrichment {
    override def isDefinedFor(dt: DataType): Boolean = dt match {
      case ArrayType(StringType, _) => true
      case _                        => false
    }
  }

  case class DoubleDefaultEnrichment(value: Double) extends DefaultEnrichment {
    override def isDefinedFor(dt: DataType): Boolean = dt == DoubleType
  }

  def noDefaultEnrichmentFoundPf = new PartialFunction[DataType, Nothing] {
    override def isDefinedAt(x: DataType): Boolean = true

    override def apply(t: DataType): Nothing = throw new NotImplementedError(
        s"No default value exists for a column of type `${t}`.")
  }

  def stringEnrichmentNotFound      = StringDefaultEnrichment("!ENRICHMENT NOT FOUND!")
  def stringArrayEnrichmentNotFound = StringSeqDefaultEnrichment(Seq(stringEnrichmentNotFound.value))
  def doubleEnrichmentNotFound      = DoubleDefaultEnrichment(-1d)
  def defaultEnrichments: Seq[DefaultEnrichment] = Seq(stringEnrichmentNotFound, stringArrayEnrichmentNotFound, doubleEnrichmentNotFound)
  def enrichmentSubstitutions: PartialFunction[DataType, Any] =  defaultEnrichments.foldRight(
    noDefaultEnrichmentFoundPf
  )((left, right) => left.pf.orElse(right))


  def fillEnrichmentDefaults(df: DataFrame): DataFrame = {
    df.na.fill(enrichmentDefaultsFillMap)
  }

  def enrichmentDefaultsFillMap: Map[String, Any] = {
    enrichedAttributesFields.map { sf =>
      val replacement = sf.dataType match {
        case StringType               => stringEnrichmentNotFound
        case ArrayType(StringType, _) => stringArrayEnrichmentNotFound
        case DoubleType               => doubleEnrichmentNotFound
        case t =>
          throw new NotImplementedError(
            s"An unexpected attribute `${sf}` found in the definition enrichment list. " +
              s"No default value exists for a column of type `${t}`.")
      }
      sf.name -> replacement
    }.toMap
  }

//  def isEnriched(cn: String) = {
//    if
//  }
}
