package pb.dictionary.extraction

import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, lit}
import pb.dictionary.extraction.ApplicationManagedProduct.UPDATED_AT

import java.io.{File, FileInputStream, FileNotFoundException}
import java.sql.Timestamp
import scala.reflect.runtime.universe.TypeTag

/** Methods used across [[Area]] implementations. */
object AreaUtils {
  // TODO: move to other utility class
  def fetchFile(path: String): FileInputStream = {
    val credentialsFile = new File(path)
    if (!credentialsFile.exists()) {
      throw new FileNotFoundException(s"Credentials file not found: ${path}")
    }
    new FileInputStream(credentialsFile)
  }

  /** Select records from [[snapshot]] with [[UPDATED_AT]] timestamp later than the latest timestamp from [[previousSnapshot]]. */
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

  def timestampToString(c: Column, format: String = "yyyy-MM-dd HH:mm:ss"): Column = date_format(c, format)
}
