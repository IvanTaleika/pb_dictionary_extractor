package pb.dictionary.extraction.bronze

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import pb.dictionary.extraction.{AreaUtils, DeltaArea}
import pb.dictionary.extraction.stage.HighlightedText

import java.sql.Timestamp

/** Stores cleansed distinct text tokens (usually individual words) with a sufficient size
  * (to eliminate accents and other garbage) from the PocketBook highlight marks.
  * In case the same token was highlighted several time, highlights info is accumulated.
  * The goal for the area is to switch operations from PocketBook highlights to text entries by
  * changing the primary key from a [[HighlightedText.pk]] to a [[CleansedText.pk]]
  * The area stores data in a managed spark metastore table backed by Delta with a [[CleansedText.pk]] primary key.
  */
class BronzeArea(
    path: String,
    timestampProvider: () => Timestamp
) extends DeltaArea[CleansedText](path) {
  import CleansedText._

  def upsert(stageSnapshot: Dataset[HighlightedText]): Dataset[CleansedText] = {
    stageSnapshot
      .transform(AreaUtils.findUpdatesByUpdateTimestamp(snapshot))
      .transform(fromStage)
      .transform(updateArea)
  }

  /** Tokenizes and normalizes PocketBook highlights, changing the PK from an [[HighlightedText.pk]] to a [[CleansedText.pk]]. */
  private def fromStage(stage: Dataset[HighlightedText]): DataFrame = {
    val tokensArrCol = "tokensArr"
    val tokenCol     = "token"
    // Punctuation is a bit messy in a PocketBook. It often sticks to words.
    // Moreover, you can only mark text separated by spaces. So it happens that instead of marking
    // a single word, you mark a series, separated by punctuation.
    // We split text by punctuation in order to not loss any meaningful information.
    val clearedStage = stage
      .withColumn(tokensArrCol, split(col(HighlightedText.TEXT), """[^\w\-\s]"""))
      .transform { df =>
        val cols = df.columns.map(col)
        df.select(cols :+ explode(col(tokensArrCol)).as(tokenCol): _*)
      }
      // Just to exclude `a`s and `s`es
      .where(length(col(tokenCol)) > 1)
      .select(
        trim(lower(col(tokenCol))) as TEXT,
        format_string("`%s` BY `%s`", col(HighlightedText.TITLE), col(HighlightedText.AUTHORS)) as BOOKS,
        to_timestamp(from_unixtime(col(HighlightedText.TIME_EDT))) as HighlightedText.TIME_EDT,
      )

    clearedStage
      .groupBy(TEXT)
      .agg(
        collect_set(BOOKS) as BOOKS,
        count("*") cast IntegerType as CleansedText.OCCURRENCES,
        min(HighlightedText.TIME_EDT) as CleansedText.FIRST_OCCURRENCE,
        max(HighlightedText.TIME_EDT) as CleansedText.LATEST_OCCURRENCE
      )
  }

  /** Saves distinct text entries, accumulating attributes for duplicates. */
  private def updateArea(textUpdates: DataFrame): Dataset[CleansedText] = {
    val updateTimestamp = timestampProvider()
    val mergeDf         = textUpdates.withColumn(UPDATED_AT, lit(updateTimestamp)).as(stagingAlias)
    deltaTable
      .as(tableName)
      // TODO: replace with pk?
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
