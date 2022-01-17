package pb.dictionary.extraction.silver

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.DeltaArea
import pb.dictionary.extraction.bronze.CleansedWord

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

// TODO: definition may be in different cases (e.g.  abbreviations like "MY")
class SilverArea(
    path: String,
    definitionApi: WordDefinitionApi,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends DeltaArea[CleansedWord, DefinedWord](path) {
  import DefinedWord._
  import spark.implicits._

  override def upsert(updates: Dataset[CleansedWord], snapshot: Dataset[CleansedWord]): Dataset[DefinedWord] = {
    updates.transform(findUndefined).transform(definitionApi.define).transform(updateArea(updates))
  }

  private def findUndefined(bronze: Dataset[CleansedWord]): Dataset[CleansedWord] = {
    bronze.join(snapshot, Seq(TEXT), "left_anti").as[CleansedWord]
  }

  private def updateArea(allUpdates: Dataset[CleansedWord])(
      newDefinitions: Dataset[DefinedWord]): Dataset[DefinedWord] = {
    val currentTimestamp = timestampProvider()
    val oldWords = allUpdates
      .as("updates")
      .join(newDefinitions.as("definitions"), col(s"updates.$TEXT") === col(s"definitions.$TEXT"), "left_anti")
    val metadataUpdate = UPDATED_AT -> lit(currentTimestamp)

    deltaTable
      .as(tableName)
      .merge(oldWords.as(stagingAlias), colDelta(TEXT) === colStaged(TEXT))
      .whenMatched()
      .update(bronzePropagatingCols.map(c => c -> colStaged(c)).toMap + metadataUpdate)
      .execute()

    newDefinitions
      .withColumn(UPDATED_AT, lit(currentTimestamp))
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(absoluteTablePath)

    val updates = snapshot.where(col(UPDATED_AT) === lit(currentTimestamp))
    updates
  }

}
