package pb.dictionary.extraction

import org.apache.spark.sql.{DataFrame, SparkSession}

// TODO: remove the interface, working just with areas
trait Publisher {
  protected val spark = SparkSession.active

  def path: String
  def publish(df: DataFrame): Unit
}
