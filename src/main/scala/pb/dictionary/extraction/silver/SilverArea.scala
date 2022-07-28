package pb.dictionary.extraction.silver

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}
import pb.dictionary.extraction.DeltaArea
import pb.dictionary.extraction.bronze.CleansedText
import pb.dictionary.extraction.utils.AreaUtils

import java.sql.Timestamp

/** Stores definitions for PocketBook highlight text tokens.
  *
  * The primary key is switched to [[DefinedText.pk]]. It ensures uniqueness for text with multiple
  * definitions and '''contain nulls''' for text without definition at all. PocketBook highlight
  * attributes are duplicated for all the exploded records.
  *
  * The goal of this area is to switch from text to definitions. [[DefinedText.pk]] attributes are
  * a minimal unit, required for words learning vocabulary.
  * The area stores data in a managed spark metastore table backed by Delta with a [[DefinedText.pk]] primary key.
  */
class SilverArea(
    path: String,
    definitionApi: TextDefinitionApi,
    timestampProvider: () => Timestamp
) extends DeltaArea[DefinedText](path) {
  import DefinedText._
  import spark.implicits._

  def upsert(bronzeSnapshot: Dataset[CleansedText]): Dataset[DefinedText] = {
    val cachedSnapshot      = snapshot.cache()
    val cachedBronzeUpdates = AreaUtils.findUpdatesByUpdateTimestamp(cachedSnapshot)(bronzeSnapshot).cache()

    val newDefinitions = findUndefined(cachedSnapshot, cachedBronzeUpdates).transform(definitionApi.define)
    val allUpdates     = buildUpdateDf(cachedSnapshot, cachedBronzeUpdates, newDefinitions)
    updateArea(allUpdates)
  }

  /** Returns text without definition, both new and already stored in the [[SilverArea]]. */
  private def findUndefined(cashedSnapshot: Dataset[DefinedText], cachedBronzeUpdates: Dataset[CleansedText]) = {
    val cachedUndefined = cashedSnapshot.where(nonDefined())

    val newUndefinedEntries = cachedBronzeUpdates.join(cashedSnapshot, CleansedText.pk, "left_anti").as[CleansedText]
    val updatedUndefinedEntries =
      cachedBronzeUpdates.join(cachedUndefined, CleansedText.pk, "left_semi").as[CleansedText]
    val newAndUpdatedUndefinedEntries = newUndefinedEntries.union(updatedUndefinedEntries)
    val undefinedEntriesWithoutUpdates = cachedUndefined
      .join(newAndUpdatedUndefinedEntries, CleansedText.pk, "left_anti")
      .select(cachedBronzeUpdates.columns.map(col): _*)
      .as[CleansedText]

    val undefinedEntries = newAndUpdatedUndefinedEntries.union(undefinedEntriesWithoutUpdates)
    undefinedEntries
  }

//  /** Combines PocketBook highlight attributes updates with enriched [[WordDefinitionApi]] attributes
//    * to create a single [[DataFrame]] for a atomic Delta MERGE operation
//    *
//    * @param existingSilverEntries updated records, already defined in the [[SilverArea]].
//    *                              Without [[WordDefinitionApi]] populated attributes
//    * @param newDefinitions text either never saved to the [[SilverArea]] or previously saved
//    *                       with definition not found. Enriched tih [[WordDefinitionApi]] attributes
//    * @return A [[DataFrame]] for Delta MERGE operation
//    */
  private def buildUpdateDf(cashedSnapshot: Dataset[DefinedText],
                            cachedBronzeUpdates: Dataset[CleansedText],
                            newDefinitions: DataFrame): Dataset[DefinedText] = {
    // TODO: add test that no nulls propagate to the area
    val normalizedNewDefinitions = DefinedText.fillEnrichmentDefaults(newDefinitions)

    val updatedDefinitions = cachedBronzeUpdates
      .join(cashedSnapshot.where(isDefined()), CleansedText.pk, "left_semi")
      .select(
        (pk.map(cashedSnapshot.apply) ++ copiedAttributes.map(cachedBronzeUpdates.apply) ++ enrichedAttributes.map(
          cashedSnapshot.apply)): _*
      )
    val currentTimestamp = timestampProvider()

    val finishedNewDefinitions =
      updatedDefinitions
        .unionByName(normalizedNewDefinitions)
        .withColumn(UPDATED_AT, lit(currentTimestamp))
        .as[DefinedText]
    finishedNewDefinitions
  }

  private def updateArea(updates: Dataset[DefinedText]): Dataset[DefinedText] = {
    deltaTable
      .as(tableName)
      .merge(updates.toDF().as(stagingAlias), mergePkMatches)
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()

    logger.info(s"Table `${fullTableName}` is updated successfully.")

    snapshot
  }

}
