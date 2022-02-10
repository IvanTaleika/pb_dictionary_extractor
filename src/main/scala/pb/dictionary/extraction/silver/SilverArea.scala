package pb.dictionary.extraction.silver

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.{AreaUtils, DeltaArea}
import pb.dictionary.extraction.bronze.CleansedText

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

// TODO: definition may be in different cases (e.g.  abbreviations like "MY")
class SilverArea(
    path: String,
    definitionApi: WordDefinitionApi,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends DeltaArea[DefinedText](path) {
  import DefinedText._
  import spark.implicits._

  def upsert(previousSnapshot: Dataset[CleansedText]): Dataset[DefinedText] = {
    val (existingDefinitions, newEntries) = findUndefined(AreaUtils.findUpdatesByUpdateTimestamp(snapshot)(previousSnapshot))
    val newDefinitions                    = definitionApi.define(newEntries)
    val allUpdates                        = buildUpdateDf(existingDefinitions, newDefinitions)
    updateArea(allUpdates)
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

  private def buildUpdateDf(existingEntries: Dataset[CleansedText], newDefinitions: DataFrame): Dataset[DefinedText] = {
    val currentTimestamp = timestampProvider()

    val dummyOldDefinitions = existingEntries
      .select(
        (
          Seq(lit(currentTimestamp) as UPDATED_AT) ++
            pkCols ++
            propagatingAttributesCols ++
            enrichedAttributesFields.map(f => lit(null) cast f.dataType as f.name)
        ): _*)
      .as[DefinedText]
    val finishedNewDefinitions = newDefinitions.withColumn(UPDATED_AT, lit(currentTimestamp)).as[DefinedText]

    dummyOldDefinitions.unionByName(finishedNewDefinitions)
  }

  private def updateArea(updates: Dataset[DefinedText]): Dataset[DefinedText] = {
    deltaTable
      .as(tableName)
      .merge(updates.toDF().as(stagingAlias), colDelta(TEXT) === colStaged(TEXT))
      .whenMatched()
      .update((propagatingAttributes :+ UPDATED_AT).map(c => c -> colStaged(c)).toMap)
      .whenNotMatched()
      .insertAll()
      .execute()

    logger.info(s"Table `${fullTableName}` is updated successfully.")

    snapshot
  }

}
