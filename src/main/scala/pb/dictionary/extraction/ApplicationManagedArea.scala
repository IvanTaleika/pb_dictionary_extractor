package pb.dictionary.extraction

import org.apache.spark.sql.Dataset

import java.nio.file.Paths
import scala.reflect.runtime.universe.TypeTag

abstract class ApplicationManagedArea[In, Out <: Product: TypeTag](path: String, format: String) extends Area[Out] {
  protected val absoluteLocation = Paths.get(path).toAbsolutePath
  val absoluteTablePath          = absoluteLocation.toUri.getPath
  val tableName                  = absoluteLocation.getFileName.toString
  val absoluteDatabasePath       = absoluteLocation.getParent.toUri.toString
  val databaseName               = absoluteLocation.getParent.getFileName.toString
  val fullTableName              = s"$databaseName.$tableName"
  initTable()

  protected def initTable(): Unit = {
    val dbCreationStmt     = s"create database if not exists $databaseName location '$absoluteDatabasePath'"
    val tableOptionsClause = if(tableOptions.nonEmpty) tableOptions.map { case (k, v) => s"$k='$v'" }.mkString("options(", ",", ")") else ""
    val tableCreationStmt =
      s"create table if not exists $fullTableName (${schema.toDDL}) using $format $tableOptionsClause location '$absoluteTablePath'"
    spark.sql(dbCreationStmt)
    spark.sql(tableCreationStmt)
  }

  protected def tableOptions = Map.empty[String, String]

  /** Upsert records from lower tier area by PK and returns new records. */
  def upsert(updates: Dataset[In], snapshot: Dataset[In]): Dataset[Out]

}
