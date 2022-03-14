package pb.dictionary.extraction.silver

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.{AreaUtils, DeltaArea}
import pb.dictionary.extraction.bronze.CleansedText

import java.sql.Timestamp

class SilverArea(
    path: String,
    definitionApi: WordDefinitionApi,
    timestampProvider: () => Timestamp
) extends DeltaArea[DefinedText](path) {
  import DefinedText._
  import spark.implicits._

  def upsert(previousSnapshot: Dataset[CleansedText]): Dataset[DefinedText] = {
    val (existingDefinitions, undefinedEntries) = findUndefined(
      AreaUtils.findUpdatesByUpdateTimestamp(snapshot)(previousSnapshot))
    val newDefinitions = definitionApi.define(undefinedEntries)
    val allUpdates     = buildUpdateDf(existingDefinitions, newDefinitions)
    updateArea(allUpdates)
  }

  private def findUndefined(bronze: Dataset[CleansedText]) = {
    val cachedBronze    = bronze.cache()
    val cachedSilver    = snapshot.cache()
    val definedSilver   = cachedSilver.where(col(NORMALIZED_TEXT).isNotNull)
    val undefinedSilver = cachedSilver.where(col(NORMALIZED_TEXT).isNull)

    val updatedDefinitions            = cachedBronze.join(definedSilver, Seq(TEXT), "left_semi").as[CleansedText]
    val newUndefinedEntries           = cachedBronze.join(cachedSilver, Seq(TEXT), "left_anti").as[CleansedText]
    val updatedUndefinedEntries       = cachedBronze.join(undefinedSilver, Seq(TEXT), "left_semi").as[CleansedText]
    val newAndUpdatedUndefinedEntries = newUndefinedEntries.union(updatedUndefinedEntries)
    val undefinedEntriesWithoutUpdates = undefinedSilver
      .join(newAndUpdatedUndefinedEntries, Seq(TEXT), "left_anti")
      .select(cachedBronze.columns.map(col): _*)
      .as[CleansedText]

    val undefinedEntries = newAndUpdatedUndefinedEntries.union(undefinedEntriesWithoutUpdates)

    (updatedDefinitions, undefinedEntries)
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
      .whenMatched(colStaged(NORMALIZED_TEXT).isNull)
      .update((propagatingAttributes :+ UPDATED_AT).map(c => c -> colStaged(c)).toMap)
      .whenMatched(colStaged(NORMALIZED_TEXT).isNotNull)
      .update((propagatingAttributes ++ enrichedAttributes ++ Seq(UPDATED_AT)).map(c => c -> colStaged(c)).toMap)
      .whenNotMatched()
      .insertAll()
      .execute()

    logger.info(s"Table `${fullTableName}` is updated successfully.")

    snapshot
  }

}
