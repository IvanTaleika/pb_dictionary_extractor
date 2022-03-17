package pb.dictionary.extraction

import java.sql.Timestamp

trait ApplicationManagedProduct extends Product {

  def updatedAt: Timestamp
}

object ApplicationManagedProduct {
  val UPDATED_AT = "updatedAt"
}

trait ApplicationManagedProductCompanion[T <: ApplicationManagedProduct] extends ProductCompanion[T] {

  final val UPDATED_AT   = ApplicationManagedProduct.UPDATED_AT
  def metadata: Seq[String] = Seq(UPDATED_AT)
}
