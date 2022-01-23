package pb.dictionary.extraction

import grizzled.slf4j.Logger
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions.format_string
import org.apache.spark.sql.types.StructType
import pb.dictionary.extraction.stage.HighlightedText.UPDATED_AT

import java.io.File
import scala.reflect.io.Directory
import scala.reflect.runtime.universe.TypeTag

abstract class Area[Out <: Product: TypeTag] {
  protected val logger   = Logger(getClass)
  protected val spark    = SparkSession.active
  val schema: StructType = Encoders.product[Out].schema

  def path: String
  def snapshot: Dataset[Out]
}

import org.apache.spark.sql.functions.{col, lit}

import java.nio.file.Paths
import java.sql.Timestamp

abstract class ApplicationManagedArea[In, Out <: Product: TypeTag](val path: String, val format: String)
    extends Area[Out] {
  protected val absoluteTableLocation = Paths.get(path).toAbsolutePath
  val absoluteTablePath               = absoluteTableLocation.toUri.getPath
  val tableName                       = absoluteTableLocation.getFileName.toString
  val absoluteDatabasePath            = absoluteTableLocation.getParent.toUri.toString
  val databaseName                    = absoluteTableLocation.getParent.getFileName.toString
  val fullTableName                   = s"$databaseName.$tableName"
  logger.info(s"Initializing `$format` table `$databaseName`.`$tableName`.")
  initTable()

  /** Upsert records from lower tier area by PK and returns new records. */
  def upsert(previousSnapshot: Dataset[In]): Dataset[Out]

  protected def tableOptions    = Map.empty[String, String]
  protected def tablePartitions = Seq.empty[String]

  protected def initTable(): Unit = {
    val dbCreationStmt = s"create database if not exists $databaseName location '$absoluteDatabasePath'"
    val tableOptionsClause =
      if (tableOptions.nonEmpty) tableOptions.map { case (k, v) => s"$k='$v'" }.mkString("options (", ",", ")") else ""
    val tablePartitionsClause =
      if (tablePartitions.nonEmpty) tablePartitions.mkString("partitioned by (", ",", ")") else ""
    val tableCreationStmt =
      s"create table if not exists $fullTableName (${schema.toDDL}) using $format $tableOptionsClause $tablePartitionsClause location '$absoluteTablePath'"
    spark.sql(dbCreationStmt)
    spark.sql(tableCreationStmt)
  }

  protected def findUpdates(previousSnapshot: Dataset[In]): Dataset[In] = {
    // TODO: move to some common interface
    val UPDATED_AT = "updatedAt"
    import spark.implicits._
    val condition = snapshot
      .select(col(UPDATED_AT))
      .as[Timestamp]
      .orderBy(col(UPDATED_AT).desc_nulls_last)
      .head(1)
      .headOption
      .map(col(UPDATED_AT) > _)
      .getOrElse(lit(true))
    previousSnapshot.where(condition)
  }

  override def snapshot: Dataset[Out] = {
    import spark.implicits._
    spark.table(fullTableName).as[Out]
  }
}

import io.delta.tables.DeltaTable

abstract class DeltaArea[In, Out <: Product: TypeTag](path: String)
    extends ApplicationManagedArea[In, Out](path, "delta") {
  protected def deltaTable                                   = DeltaTable.forPath(absoluteTablePath)
  protected def stagingAlias                                 = "staging"
  protected def colFromTable(tableAlias: String)(cn: String) = col(s"$tableAlias.$cn")
  protected def colDelta(cn: String)                         = colFromTable(tableName)(cn)
  protected def colStaged(cn: String)                        = colFromTable(stagingAlias)(cn)
}

abstract class CsvSnapshotsArea[In, Out <: Product: TypeTag](path: String, timestampProvider: () => Timestamp)
    extends ApplicationManagedArea[In, Out](path, "csv") {

  protected def outputFiles: Option[Int] = Option.empty

  override protected def tableOptions    = Map("multiline" -> "true", "header" -> "true", "mode" -> "FAILFAST")
  override protected def tablePartitions = Seq(UPDATED_AT)

  override protected def initTable(): Unit = {
    super.initTable()
    updateMetadata()
  }

  protected def updateMetadata(): Unit = {
    if (new Directory(new File(absoluteTablePath)).exists) {
      spark.sql(s"msck repair table ${fullTableName}")
    }
  }

  protected def writeSnapshot(df: DataFrame): Dataset[Out] = {
    val updateTimestamp = timestampProvider()

    val outDf = df
      .withColumn(UPDATED_AT, lit(updateTimestamp))
      .transform(df => outputFiles.map(df.coalesce).getOrElse(df))

    outDf.write
      .partitionBy(tablePartitions: _*)
      .mode(SaveMode.Append)
      .format(format)
      .saveAsTable(fullTableName)
    updateMetadata()
    logger.info(s"Table `${fullTableName}` is updated successfully.")
    snapshot
  }

  override def snapshot: Dataset[Out] = {
    import spark.implicits._
    val history = super.snapshot
    // TODO: move latest timestamp selection to some common interface?
    val latestSnapshotTimestamp = history.select(UPDATED_AT).as[Timestamp].orderBy(col(UPDATED_AT).desc).head(1)
    latestSnapshotTimestamp.headOption.map(t => history.where(col(UPDATED_AT) === t)).getOrElse(history)
  }

  protected def timestampToCsvString(c: Column) = format_string("yyyy-MM-dd HH:mm:ss", c)

}
