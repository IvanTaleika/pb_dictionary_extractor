package pb.dictionary.extraction.golden

import org.apache.spark.sql.DataFrame

trait UsageFrequencyApi {

  def findUsageFrequency(df: DataFrame): DataFrame
}
