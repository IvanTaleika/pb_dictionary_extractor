package pb.dictionary.extraction

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import pb.dictionary.extraction.ApplicationManagedProduct.UPDATED_AT

import java.sql.Timestamp
import scala.reflect.runtime.universe.TypeTag

object AreaUtils {
  def findUpdatesByUpdateTimestamp[In <: ApplicationManagedProduct: TypeTag, Out <: ApplicationManagedProduct: TypeTag](
      snapshot: Dataset[Out])(previousSnapshot: Dataset[In]): Dataset[In] = {
    val spark = SparkSession.active
    import spark.implicits._
    val condition = snapshot
      .select(col(UPDATED_AT))
      .as[Timestamp]
      .orderBy(col(UPDATED_AT).desc_nulls_last)
      .head(1)
      .headOption
      .map(col(UPDATED_AT) > _)
      .getOrElse(lit(true))
    previousSnapshot.where(condition)
  }
}
