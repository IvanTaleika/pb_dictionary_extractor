package pb.dictionary.extraction.publish

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import pb.dictionary.extraction.Publisher
import pb.dictionary.extraction.golden.DictionaryRecord._

import java.time.{LocalDateTime, ZonedDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
// We can also use Google API in foreach/map function https://developers.google.com/sheets/api/guides/values
// or excel spark connector https://github.com/crealytics/spark-excel
class GoogleSheets(val path: String, publishTime: LocalDateTime = ZonedDateTime.now(ZoneOffset.UTC).toLocalDateTime)
    extends Publisher {

  // TODO: quoter, delim etc.
  private val publishOptions = Map(
    "header" -> "true"
  )

  override def publish(df: DataFrame): Unit = {
    val path = writeSnapshot(fromGolden(df))
    // TODO: send file to google sheets
  }

  private def fromGolden(golden: DataFrame) = {
    golden
      .select(
        // column order
        col(NORMALIZED_TEXT),
        col(PHONETIC),
        col(PART_OF_SPEECH),
        col(DEFINITION),
        col(TRANSLATION),
        col(EXAMPLE),
        array_join(col(SYNONYMS), ",").as(SYNONYMS),
        array_join(col(ANTONYMS), ",").as(ANTONYMS),
        col(OCCURRENCES),
        format_string("yyyy-MM-dd HH:mm:ss", col(FIRST_OCCURRENCE)).as(FIRST_OCCURRENCE),
        format_string("yyyy-MM-dd HH:mm:ss", col(LATEST_OCCURRENCE)).as(LATEST_OCCURRENCE),
//        col(FIRST_OCCURRENCE).cast(StringType),
//        col(LATEST_OCCURRENCE).cast(StringType),
        array_join(col(BOOKS), ",").as(BOOKS),
        array_join(col(FORMS), ",").as(FORMS)
      )
  }

  private def writeSnapshot(df: DataFrame): String = {
    val publishTimeString = publishTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss"))
    val outputPath        = String.join("/", path, s"exportTime=$publishTimeString")
    df.write.format("csv").options(publishOptions).save(outputPath)
    outputPath
  }
}
