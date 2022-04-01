package pb.dictionary.extraction.silver

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.{AreaUtils, DeltaArea}
import pb.dictionary.extraction.bronze.CleansedText

import java.sql.Timestamp

// TODO: this should change PK no normalized text + definition
/** Stores definitions for PocketBook highlight text tokens. [[CleansedText]] records are normalized
  * and possibly exploded by [[WordDefinitionApi]], primary key is switched to [[DefinedText.pk]] to ensure
  * uniqueness for words with multiple definitions. PocketBook highlight attributes are duplicated for all the
  * exploded records.
  * The goal of this area is to switch from text to definitions. [[DefinedText.pk]] attributes are
  * a minimal unit, required for words learning vocabulary.
  * The area stores data in a managed spark metastore table backed by Delta with a [[DefinedText.pk]] primary key.
  */
class SilverArea(
    path: String,
    definitionApi: WordDefinitionApi,
    timestampProvider: () => Timestamp
) extends DeltaArea[DefinedText](path) {
  import DefinedText._
  import spark.implicits._

  def upsert(bronzeSnapshot: Dataset[CleansedText]): Dataset[DefinedText] = {
    val (existingDefinitions, undefinedEntries) = findUndefined(
      AreaUtils.findUpdatesByUpdateTimestamp(snapshot)(bronzeSnapshot))
    val newDefinitions = definitionApi.define(undefinedEntries)
    val allUpdates     = buildUpdateDf(existingDefinitions, newDefinitions)
    updateArea(allUpdates)
  }

  /** Returns text without definition, both new and already stored in the [[SilverArea]], to retry definition query. */
  private def findUndefined(bronzeUpdates: Dataset[CleansedText]) = {
    val cachedBronze    = bronzeUpdates.cache()
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

  /** Combines PocketBook highlight attributes updates with enriched [[WordDefinitionApi]] attributes
    * to create a single [[DataFrame]] for a atomic Delta MERGE operation
    *
    * @param existingSilverEntries updated records, already defined in the [[SilverArea]].
    *                              Without [[WordDefinitionApi]] populated attributes
    * @param newDefinitions text either never saved to the [[SilverArea]] or previously saved
    *                       with definition not found. Enriched tih [[WordDefinitionApi]] attributes
    * @return A [[DataFrame]] for Delta MERGE operation
    */
  private def buildUpdateDf(existingSilverEntries: Dataset[CleansedText],
                            newDefinitions: DataFrame): Dataset[DefinedText] = {
    val currentTimestamp = timestampProvider()

    val dummyOldDefinitions = existingSilverEntries
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
