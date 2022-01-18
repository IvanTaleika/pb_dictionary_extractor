package pb.dictionary.extraction

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, lit}

import java.nio.file.Paths
import java.sql.Timestamp
import scala.reflect.runtime.universe.TypeTag

abstract class ApplicationManagedArea[In, Out <: Product: TypeTag](val path: String, format: String) extends Area[Out] {
  protected val absoluteLocation = Paths.get(path).toAbsolutePath
  val absoluteTablePath          = absoluteLocation.toUri.getPath
  val tableName                  = absoluteLocation.getFileName.toString
  val absoluteDatabasePath       = absoluteLocation.getParent.toUri.toString
  val databaseName               = absoluteLocation.getParent.getFileName.toString
  val fullTableName              = s"$databaseName.$tableName"
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

}
