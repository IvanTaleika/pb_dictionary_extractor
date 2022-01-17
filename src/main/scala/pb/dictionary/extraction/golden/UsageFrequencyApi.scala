package pb.dictionary.extraction.golden

import org.apache.spark.sql.DataFrame

trait UsageFrequencyApi {

  // TODO: typesafe?
  def findUsageFrequency(df: DataFrame): DataFrame
}
