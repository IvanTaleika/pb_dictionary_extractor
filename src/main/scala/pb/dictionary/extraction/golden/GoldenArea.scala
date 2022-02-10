package pb.dictionary.extraction.golden

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import pb.dictionary.extraction.{AreaUtils, DeltaArea}
import pb.dictionary.extraction.silver.DefinedText

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

class GoldenArea(
    path: String,
    dictionaryTranslationApi: DictionaryTranslationApi,
    usageFrequencyApi: UsageFrequencyApi,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends DeltaArea[DictionaryRecord](path) {
  import DictionaryRecord._
  import spark.implicits._

  private def pkMatches(t1: String, t2: String) =
    pk.map(cn => colFromTable(t1)(cn) === colFromTable(t2)(cn)).reduce(_ && _)

  def upsert(silverSnapshot: Dataset[DefinedText]): Dataset[DictionaryRecord] = {
    import spark.implicits._
    val updatedDefinitions = silverSnapshot
      .transform(AreaUtils.findUpdatesByUpdateTimestamp(snapshot))
      .transform(
        definedUpdates =>
          silverSnapshot
            .as("silverSnapshot")
            .join(definedUpdates.as("definedUpdates"), pkMatches("silverSnapshot", "definedUpdates"), "left_semi")
      )
      .as[DefinedText]
    val groupedDefinitions           = fromSilver(updatedDefinitions)
    val (updatedEntries, newEntries) = findNew(groupedDefinitions)
    val newDictionaryRecords = newEntries
      .transform(df => dictionaryTranslationApi.translate(df))
      .transform(df => usageFrequencyApi.findUsageFrequency(df))
    val allUpdates = buildUpdateDf(updatedEntries, newDictionaryRecords)
    updateArea(allUpdates)
  }

  private def findNew(groupedDefinitions: DataFrame) = {
    val newDefinitions = groupedDefinitions
      .as("groupedDefinitions")
      .join(snapshot.as("snapshot"), pkMatches("groupedDefinitions", "snapshot"), "left_anti")
    val oldDefinitions = groupedDefinitions
      .as("groupedDefinitions")
      .join(newDefinitions.as("newDefinitions"), pkMatches("groupedDefinitions", "newDefinitions"), "left_anti")
    (oldDefinitions, newDefinitions)
  }

  private[golden] def fromSilver(updatedDefinitions: Dataset[DefinedText]): DataFrame = {
    val pkCols               = pk.map(col)
    val updateTimeWindow     = Window.partitionBy(pkCols: _*).orderBy(col(DefinedText.UPDATED_AT).desc_nulls_last)
    val definitionAttributes = Seq(SYNONYMS, ANTONYMS, PHONETIC, PART_OF_SPEECH, EXAMPLES)
    val latestDefinitionAttributes = definitionAttributes
      .foldLeft(updatedDefinitions.toDF)((df, cn) => df.withColumn(cn, first(cn).over(updateTimeWindow)))
    val newGoldenRecords = latestDefinitionAttributes
      .groupBy((pkCols ++ definitionAttributes.map(col)): _*)
      .agg(
        collect_set(DefinedText.TEXT) as DictionaryRecord.FORMS,
        flatten(collect_set(col(DefinedText.BOOKS))) as DictionaryRecord.BOOKS,
        sum(DefinedText.OCCURRENCES) cast IntegerType as DictionaryRecord.OCCURRENCES,
        min(DefinedText.FIRST_OCCURRENCE) as DictionaryRecord.FIRST_OCCURRENCE,
        max(DefinedText.LATEST_OCCURRENCE) as DictionaryRecord.LATEST_OCCURRENCE,
      )
      .withColumn(BOOKS, array_distinct(col(BOOKS)))
    newGoldenRecords
  }

  private def buildUpdateDf(existingEntries: DataFrame, newDefinitions: DataFrame): Dataset[DictionaryRecord] = {
    val currentTimestamp = timestampProvider()

    val dummyOldDefinitions = existingEntries
      .select(
        (
          Seq(lit(currentTimestamp) as UPDATED_AT) ++
            pkCols ++
            propagatingAttributesCols ++
            enrichedAttributesFields.map(f => lit(null) cast f.dataType as f.name)
        ): _*)
      .as[DictionaryRecord]
    val finishedNewDefinitions = newDefinitions.withColumn(UPDATED_AT, lit(currentTimestamp)).as[DictionaryRecord]

    dummyOldDefinitions.unionByName(finishedNewDefinitions)
  }

  private[golden] def updateArea(updates: Dataset[DictionaryRecord]): Dataset[DictionaryRecord] = {
    deltaTable
      .as(tableName)
      .merge(updates.toDF().as(stagingAlias), pkMatches(tableName, stagingAlias))
      .whenMatched()
      .update((propagatingAttributes :+ UPDATED_AT).map(c => c -> colStaged(c)).toMap)
      .whenNotMatched()
      .insertAll()
      .execute()

    logger.info(s"Table `${fullTableName}` is updated successfully.")
    snapshot
  }
}
