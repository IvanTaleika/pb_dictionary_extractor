package pb.dictionary.extraction.bronze

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import pb.dictionary.extraction.DeltaArea
import pb.dictionary.extraction.stage.HighlightedSentence

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

class BronzeArea(
    path: String,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends DeltaArea[HighlightedSentence, CleansedWord](path) {
  import CleansedWord._

  override def upsert(previousSnapshot: Dataset[HighlightedSentence]): Dataset[CleansedWord] = {
    previousSnapshot.transform(findUpdates).transform(fromStage).transform(updateArea)
  }

  private def fromStage(stage: Dataset[HighlightedSentence]): DataFrame = {
    val tokensArrCol = "tokensArr"
    val tokenCol     = "token"
    // Punctuation is a bit messy in a PocketBook. It often sticks to words.
    // Moreover, you can only mark text separated by spaces. So it happens that instead of marking
    // a single word, you mark a series, separated by punctuation.
    // We split text by punctuation in order to not loss any meaningful information.
    val clearedStage = stage
      .withColumn(tokensArrCol, split(col(HighlightedSentence.TEXT), """[^\w\-\s]"""))
      .transform { df =>
        val cols = df.columns.map(col)
        df.select(cols :+ explode(col(tokensArrCol)).as(tokenCol): _*)
      }
      // Just to exclude `a`s and `s`es
      .where(length(col(tokenCol)) > 1)
      .select(
        trim(lower(col(tokenCol))) as TEXT,
        format_string("`%s` BY `%s`", col(HighlightedSentence.TITLE), col(HighlightedSentence.AUTHORS)) as BOOKS,
        to_timestamp(from_unixtime(col(HighlightedSentence.TIME_EDT))) as HighlightedSentence.TIME_EDT,
      )

    clearedStage
      .groupBy(TEXT)
      .agg(
        collect_set(BOOKS) as BOOKS,
        count("*") cast IntegerType as CleansedWord.OCCURRENCES,
        min(HighlightedSentence.TIME_EDT) as CleansedWord.FIRST_OCCURRENCE,
        max(HighlightedSentence.TIME_EDT) as CleansedWord.LATEST_OCCURRENCE
      )
  }

  private def updateArea(words: DataFrame): Dataset[CleansedWord] = {
    val updateTimestamp = timestampProvider()
    val mergeDf         = words.withColumn(UPDATED_AT, lit(updateTimestamp)).as(stagingAlias)
    deltaTable
      .as(tableName)
      .merge(mergeDf, colDelta(TEXT) === colStaged(TEXT))
      .whenMatched()
      .update(Map(
        BOOKS             -> array_union(colStaged(BOOKS), colDelta(BOOKS)),
        OCCURRENCES       -> (colStaged(OCCURRENCES) + colDelta(OCCURRENCES)),
        FIRST_OCCURRENCE  -> least(colDelta(FIRST_OCCURRENCE), colStaged(FIRST_OCCURRENCE)),
        LATEST_OCCURRENCE -> greatest(colDelta(LATEST_OCCURRENCE), colStaged(LATEST_OCCURRENCE)),
        UPDATED_AT        -> colStaged(UPDATED_AT)
      ))
      .whenNotMatched()
      .insertAll()
      .execute()
    logger.info(s"Table `${fullTableName}` is updated successfully.")

    snapshot
  }
}
