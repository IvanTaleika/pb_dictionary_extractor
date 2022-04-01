package pb.dictionary.extraction.golden

import org.apache.spark.sql.DataFrame

/** Enriches [[DataFrame]] with usage metric, showing how often the word is used comparing with other words. */
trait UsageFrequencyApi {

  /** Enriches [[DataFrame]] with [[RichDefinedText.USAGE]] column. */
  def findUsageFrequency(df: DataFrame): DataFrame
}
