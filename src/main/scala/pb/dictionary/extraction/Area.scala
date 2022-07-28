package pb.dictionary.extraction

import grizzled.slf4j.Logger
import org.apache.spark.sql._
import pb.dictionary.extraction.sql.functions._
import org.apache.spark.sql.types.StructType
import pb.dictionary.extraction.ApplicationManagedProduct._

import java.io.File
import scala.reflect.io.Directory

/** An area that stores data, without constraints on data format, metastore aliases, lifecycle and etc..
  * The most generic data storage definition in a PocketBook dictionary application.
  */
abstract class Area[Out <: Product: ProductCompanion] {
  protected val logger   = Logger(getClass)
  protected val spark    = SparkSession.active
  val areaDescriptor     = implicitly[ProductCompanion[Out]]
  val schema: StructType = areaDescriptor.schema

  def path: String

  /** Returns the current area content. */
  def snapshot: Dataset[Out]
}

import org.apache.spark.sql.functions.{col, lit}

import java.nio.file.Paths
import java.sql.Timestamp

/** A data storage area that is created and managed by the `pb_dictionary_extractor` application.
  * This area represents a managed spark table stored in a [[path]] path. Both database and table are
  * available right after the object initialization (crated in the constructor, if does not exist already)
  *
  * @param path area location on the filesystem. The last folder name is equal to the table name
  *             and its parent name is equal to the database name.
  * @param format file format used to store the data.
  * @tparam Out A [[ApplicationManagedProduct]] corresponding to the Area schema.
  */
abstract class ApplicationManagedArea[Out <: ApplicationManagedProduct: ApplicationManagedProductCompanion](
    val path: String,
    val format: String)
    extends Area[Out] {

  protected val absoluteTableLocation = Paths.get(path).toAbsolutePath
  val absoluteTablePath               = absoluteTableLocation.toUri.getPath
  val tableName                       = absoluteTableLocation.getFileName.toString
  val absoluteDatabasePath            = absoluteTableLocation.getParent.toUri.toString
  val databaseName                    = absoluteTableLocation.getParent.getFileName.toString
  val fullTableName                   = s"$databaseName.$tableName"
  logger.info(s"Initializing `$format` table `$databaseName`.`$tableName`.")
  initTable()

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

  override def snapshot: Dataset[Out] = {
    import areaDescriptor.implicits._
    import spark.implicits._

    spark.table(fullTableName).as[Out]
  }
}

import io.delta.tables.DeltaTable

/** An [[ApplicationManagedArea]] backed by the Delta format. */
abstract class DeltaArea[Out <: ApplicationManagedProduct: ApplicationManagedProductCompanion](path: String)
    extends ApplicationManagedArea[Out](path, "delta") {
  protected def deltaTable            = DeltaTable.forPath(absoluteTablePath)
  protected def stagingAlias          = "staging"
  protected def colDelta(cn: String)  = colFromTable(tableName)(cn)
  protected def colStaged(cn: String) = colFromTable(stagingAlias)(cn)
  protected def mergePkMatches        = areaDescriptor.pkMatches(tableName, stagingAlias)
}

/** An [[ApplicationManagedArea]] backed by the CSV format. */
abstract class CsvArea[Out <: ApplicationManagedProduct: ApplicationManagedProductCompanion](
    path: String,
    timestampProvider: () => Timestamp)
    extends ApplicationManagedArea[Out](path, "csv") {

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

  protected def write(df: DataFrame): Dataset[Out] = {
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

}

/**
  * An [[CsvArea]] where a full new snapshot is created on each write.
  * [[snapshot]] method returns records with the latest [[ApplicationManagedProduct.UPDATED_AT]] value,
  * instead of the whole table content.
  */
abstract class CsvSnapshotsArea[Out <: ApplicationManagedProduct: ApplicationManagedProductCompanion](
    path: String,
    timestampProvider: () => Timestamp)
    extends CsvArea[Out](path, timestampProvider) {

  override def snapshot: Dataset[Out] = {
    import spark.implicits._
    val history                 = super.snapshot
    val latestSnapshotTimestamp = history.select(UPDATED_AT).as[Timestamp].orderBy(col(UPDATED_AT).desc).head(1)
    latestSnapshotTimestamp.headOption.map(t => history.where(col(UPDATED_AT) === t)).getOrElse(history)
  }
}

case class InvalidAreaStateException(message: String, cause: Throwable = null)
    extends PbDictionaryException(message, cause)
