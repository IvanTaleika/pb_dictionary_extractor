package pb.dictionary.extraction.golden

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import pb.dictionary.extraction.DeltaArea
import pb.dictionary.extraction.silver.DefinedText
import pb.dictionary.extraction.utils.AreaUtils

import java.sql.Timestamp

/** Stores definitions enriched with translations and usage attributes.
  * The goal of this area is to enrich a minimal learning [[DefinedText.pk]] unit with attributes
  * that can ease learning process.
  * The area stores data in a managed spark metastore table backed by Delta with a [[VocabularyRecord.pk]] primary key.
  */
class GoldenArea(
    path: String,
    dictionaryTranslationApi: DictionaryTranslationApi,
    usageFrequencyApi: UsageFrequencyApi,
    timestampProvider: () => Timestamp
) extends DeltaArea[VocabularyRecord](path) {
  import VocabularyRecord._
  import spark.implicits._

  def upsert(silverSnapshot: Dataset[DefinedText]): Dataset[VocabularyRecord] = {
    val updatedTextDefinitions               = findUpdatedDefinitions(silverSnapshot)
    val groupedDefinitions                   = fromSilver(updatedTextDefinitions)
    val (updatedDefinitions, newDefinitions) = classifyDefinitions(groupedDefinitions)
    val newDictionaryRecords = newDefinitions
      .transform(df => dictionaryTranslationApi.translate(df))
      .transform(df => usageFrequencyApi.findUsageFrequency(df))
    val allUpdates = buildUpdateDf(updatedDefinitions, newDictionaryRecords)
    updateArea(allUpdates)
  }

  private def findUpdatedDefinitions(silverSnapshot: Dataset[DefinedText]): Dataset[DefinedText] = {
    val definedSilver = silverSnapshot.where(DefinedText.isDefined()).cache()

    val updatedDefinitions = definedSilver
      .transform(AreaUtils.findUpdatesByUpdateTimestamp(snapshot))
      .transform(
        // it is possible that a new text form was added to the SilverArea that matches already existing
        // vocabulary record (same `VocabularyRecord.pk`). We must select all the records with this definition
        // in order to calculated PocketBook metrics correctly.
        definedUpdates =>
          definedSilver
            .as("silverSnapshot")
            .join(definedUpdates.as("definedUpdates"), pkMatches("silverSnapshot", "definedUpdates"), "left_semi")
      )
    updatedDefinitions.as[DefinedText]
  }

  private def classifyDefinitions(groupedDefinitions: DataFrame) = {
    val classifiedDefinitions = groupedDefinitions
      .as(stagingAlias)
      .join(snapshot.as(tableName), mergePkMatches, "left_outer")
    val newDefinitions = classifiedDefinitions
      .where(colDelta(NORMALIZED_TEXT).isNull)
      .select(groupedDefinitions.columns.map(colStaged): _*)
    val updatedVocabularyRecords = classifiedDefinitions
      .where(colDelta(NORMALIZED_TEXT).isNotNull)
      .select((pkCols(colDelta) ++ transformedAttributesCols(colStaged) ++ enrichedAttributesCols(colDelta)): _*)
    (updatedVocabularyRecords, newDefinitions)
  }

  private[golden] def fromSilver(updatedDefinitions: Dataset[DefinedText]): DataFrame = {
    val updateTimeWindow     = Window.partitionBy((pkCols()): _*).orderBy(col(DefinedText.UPDATED_AT).desc_nulls_last)
    val definitionAttributes = DefinedText.enrichedAttributes.toSet -- pk.toSet
    // In case we have several text forms, we want to use definition attributes from the latest one
    // cause dictionary can be updated and an API will return richer responses.
    val latestDefinitionAttributes = definitionAttributes
      .foldLeft(updatedDefinitions.toDF)((df, cn) => df.withColumn(cn, first(cn).over(updateTimeWindow)))
    val newGoldenRecords = latestDefinitionAttributes
      .groupBy((pkCols() ++ definitionAttributes.map(col)): _*)
      .agg(
        collect_set(DefinedText.TEXT) as VocabularyRecord.FORMS,
        flatten(collect_set(col(DefinedText.BOOKS))) as VocabularyRecord.BOOKS,
        sum(DefinedText.OCCURRENCES) cast IntegerType as VocabularyRecord.OCCURRENCES,
        min(DefinedText.FIRST_OCCURRENCE) as VocabularyRecord.FIRST_OCCURRENCE,
        max(DefinedText.LATEST_OCCURRENCE) as VocabularyRecord.LATEST_OCCURRENCE,
      )
      .withColumn(BOOKS, array_distinct(col(BOOKS)))
    newGoldenRecords
  }

  private def buildUpdateDf(existingEntries: DataFrame, newDefinitions: DataFrame): Dataset[VocabularyRecord] = {
    val currentTimestamp = timestampProvider()

    val dummyOldDefinitions = existingEntries
      .select(
        (
          Seq(lit(currentTimestamp) as UPDATED_AT) ++
            pkCols() ++
            copiedAttributesCols ++
            enrichedAttributesFields.map(f => lit(null) cast f.dataType as f.name)
        ): _*)
      .as[VocabularyRecord]
    val finishedNewDefinitions = newDefinitions.withColumn(UPDATED_AT, lit(currentTimestamp)).as[VocabularyRecord]

    dummyOldDefinitions.unionByName(finishedNewDefinitions)
  }

  private[golden] def updateArea(updates: Dataset[VocabularyRecord]): Dataset[VocabularyRecord] = {
    deltaTable
      .as(tableName)
      .merge(updates.toDF().as(stagingAlias), mergePkMatches)
      .whenMatched()
      .update((copiedAttributes :+ UPDATED_AT).map(c => c -> colStaged(c)).toMap)
      .whenNotMatched()
      .insertAll()
      .execute()

    logger.info(s"Table `${fullTableName}` is updated successfully.")
    snapshot
  }
}
