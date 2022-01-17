package pb.dictionary.extraction

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Publisher {
  protected val spark = SparkSession.active

  def path: String
  def publish(df: DataFrame): Unit
}
