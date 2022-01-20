package pb.dictionary.extraction.silver

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.DeltaArea
import pb.dictionary.extraction.bronze.CleansedText

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

// TODO: definition may be in different cases (e.g.  abbreviations like "MY")
class SilverArea(
    path: String,
    definitionApi: WordDefinitionApi,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends DeltaArea[CleansedText, DefinedText](path) {
  import DefinedText._
  import spark.implicits._

  override def upsert(previousSnapshot: Dataset[CleansedText]): Dataset[DefinedText] = {
    val (oldDefinitions, newDefinitions) = findUndefined(previousSnapshot.transform(findUpdates))
    newDefinitions.transform(definitionApi.define).transform(updateArea(oldDefinitions))
  }

  // TODO: retry definition search for old words without definitions?
  private def findUndefined(bronze: Dataset[CleansedText]) = {
    val newDefinitions = bronze.join(snapshot, Seq(TEXT), "left_anti").as[CleansedText]
    val oldDefinitions = bronze
      .as("updates")
      .join(
        newDefinitions.as("definitions"),
        col(s"updates.$TEXT") === col(s"definitions.$TEXT"),
        "left_anti"
      )
      .as[CleansedText]
    (oldDefinitions, newDefinitions)
  }

  private def updateArea(oldDefinitions: Dataset[CleansedText])(
      newDefinitions: Dataset[DefinedText]): Dataset[DefinedText] = {
    val currentTimestamp = timestampProvider()

    val metadataUpdate = UPDATED_AT -> lit(currentTimestamp)

    // TODO: refactor to a single merge to make the operation atomic
    deltaTable
      .as(tableName)
      .merge(oldDefinitions.toDF().as(stagingAlias), colDelta(TEXT) === colStaged(TEXT))
      .whenMatched()
      .update(bronzePropagatingCols.map(c => c -> colStaged(c)).toMap + metadataUpdate)
      .execute()

    newDefinitions
      .withColumn(UPDATED_AT, lit(currentTimestamp))
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(absoluteTablePath)

    logger.info(s"Table `${fullTableName}` is updated successfully.")

    snapshot
  }

}
