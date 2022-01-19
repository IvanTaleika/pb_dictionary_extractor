package pb.dictionary.extraction.golden

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import pb.dictionary.extraction.DeltaArea
import pb.dictionary.extraction.silver.DefinedWord

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

class GoldenArea(
    path: String,
    dictionaryTranslationApi: DictionaryTranslationApi,
    usageFrequencyApi: UsageFrequencyApi,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends DeltaArea[DefinedWord, DictionaryRecord](path) {
  import DictionaryRecord._

  private def pkMatches(t1: String, t2: String) =
    pk.map(cn => colFromTable(t1)(cn) === colFromTable(t2)(cn)).reduce(_ && _)
  private val deltaPkMatches = pkMatches(tableName, stagingAlias)

  override def upsert(silverSnapshot: Dataset[DefinedWord]): Dataset[DictionaryRecord] = {
    import spark.implicits._
    val updatedDefinitions = silverSnapshot
      .transform(findUpdates)
      .transform(
        definedUpdates =>
          silverSnapshot
            .as("silverSnapshot")
            .join(definedUpdates.as("definedUpdates"), pkMatches("silverSnapshot", "definedUpdates"), "left_semi")
      )
      .as[DefinedWord]
    val groupedDefinitions = fromSilver(updatedDefinitions)
    val (oldDefinitions, newDefinitions) = findNew(groupedDefinitions)
    val newDictionaryRecords = newDefinitions
      .transform(df => dictionaryTranslationApi.translate(df))
      .transform(df => usageFrequencyApi.findUsageFrequency(df))
    updateArea(oldDefinitions)(newDictionaryRecords)
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

  private[golden] def fromSilver(updatedDefinitions: Dataset[DefinedWord]): DataFrame = {
    val pkCols               = pk.map(col)
    val updateTimeWindow     = Window.partitionBy(pkCols: _*).orderBy(col(DefinedWord.UPDATED_AT).desc_nulls_last)
    val definitionAttributes = Seq(SYNONYMS, ANTONYMS, PHONETIC, PART_OF_SPEECH, EXAMPLE)
    val latestDefinitionAttributes = definitionAttributes
      .foldLeft(updatedDefinitions.toDF)((df, cn) => df.withColumn(cn, first(cn).over(updateTimeWindow)))
    val newGoldenRecords = latestDefinitionAttributes
      .groupBy((pkCols ++ definitionAttributes.map(col)): _*)
      .agg(
        collect_set(DefinedWord.TEXT) as DictionaryRecord.FORMS,
        flatten(collect_set(col(DefinedWord.BOOKS))) as DictionaryRecord.BOOKS,
        sum(DefinedWord.OCCURRENCES) cast IntegerType as DictionaryRecord.OCCURRENCES,
        min(DefinedWord.FIRST_OCCURRENCE) as DictionaryRecord.FIRST_OCCURRENCE,
        max(DefinedWord.LATEST_OCCURRENCE) as DictionaryRecord.LATEST_OCCURRENCE,
      )
      .withColumn(BOOKS, array_distinct(col(BOOKS)))
    newGoldenRecords
  }

  private[golden] def updateArea(oldDefinitions: DataFrame)(newDefinitions: DataFrame): Dataset[DictionaryRecord] = {
    val currentTimestamp = timestampProvider()

    val metadataUpdate = UPDATED_AT -> lit(currentTimestamp)

    deltaTable
      .as(tableName)
      .merge(oldDefinitions.as(stagingAlias), deltaPkMatches)
      .whenMatched()
      .update(silverPropagatingCols.map(c => c -> colStaged(c)).toMap + metadataUpdate)
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
