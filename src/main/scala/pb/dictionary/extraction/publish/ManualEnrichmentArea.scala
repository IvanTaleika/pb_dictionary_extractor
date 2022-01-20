package pb.dictionary.extraction.publish

import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.silver.DefinedText
import pb.dictionary.extraction.ApplicationManagedArea

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

// TODO: write directly to Google Docs? Export csv programmatically? Rename output file?
// TODO: do not export words already defined in the publish
class ManualEnrichmentArea(
    path: String,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends ApplicationManagedArea[DefinedText, UndefinedText](path, "csv") {
  import UndefinedText._
  override protected def tableOptions = Map("multiline" -> "true", "header" -> "true", "mode" -> "FAILFAST")

  override protected def initTable(): Unit = {
    super.initTable()
    spark.sql(s"msck repair table ${fullTableName}")
  }

  override def upsert(silverSnapshot: Dataset[DefinedText]): Dataset[UndefinedText] = {
    silverSnapshot.transform(fromSilver).transform(overwriteArea)
  }

  private def fromSilver(definedText: Dataset[DefinedText]) = {
    definedText
      .filter(col(DefinedText.DEFINITION).isNull)
      .select(
        col(TEXT),
        col(OCCURRENCES),
        array_join(col(BOOKS), ",") as BOOKS,
        formatTimestamp(col(FIRST_OCCURRENCE)) as FIRST_OCCURRENCE,
        formatTimestamp(col(LATEST_OCCURRENCE)) as LATEST_OCCURRENCE,
      )
  }

  private def overwriteArea(undefinedText: DataFrame): Dataset[UndefinedText] = {
    val updateTimestamp = timestampProvider()

    undefinedText
      .withColumn(UPDATED_AT, formatTimestamp(lit(updateTimestamp)))
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .saveAsTable(fullTableName)
    spark.sql(s"msck repair table ${fullTableName}")
    logger.info(s"Table `${fullTableName}` is updated successfully.")
    snapshot
  }

  private def formatTimestamp(c: Column) = format_string("yyyy-MM-dd HH:mm:ss", c)

}
