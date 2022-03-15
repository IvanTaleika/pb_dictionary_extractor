package pb.dictionary.extraction

import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, lit}
import pb.dictionary.extraction.ApplicationManagedProduct.UPDATED_AT

import java.sql.Timestamp
import scala.reflect.runtime.universe.TypeTag

object AreaUtils {
  def findUpdatesByUpdateTimestamp[In <: ApplicationManagedProduct: TypeTag, Out <: ApplicationManagedProduct: TypeTag](
      previousSnapshot: Dataset[Out])(snapshot: Dataset[In]): Dataset[In] = {
    val spark = SparkSession.active
    import spark.implicits._
    val condition = previousSnapshot
      .select(col(UPDATED_AT))
      .as[Timestamp]
      .orderBy(col(UPDATED_AT).desc_nulls_last)
      .head(1)
      .headOption
      .map(col(UPDATED_AT) > _)
      .getOrElse(lit(true))
    snapshot.where(condition)
  }

  def timestampToString(c: Column, format: String = "yyyy-MM-dd HH:mm:ss") = date_format(c, format)

}
