package pb.dictionary.extraction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import grizzled.slf4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{array_sort, col}
import org.mockito.ArgumentMatchers
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import java.io.File
import java.sql.Timestamp
import java.time.ZonedDateTime
import scala.reflect.io.Directory
import scala.reflect.ClassTag

abstract class TestBase
    extends FunSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
    with MockFactory
    with DataFrameSuiteBase
    with AppendedClues {
  val appName          = "pb_dictionary_extractor_tests"
  val warehousePath    = "target/spark-warehouse"
  protected val logger = Logger(getClass)

  override def conf: SparkConf =
    super.conf
      .setMaster("local[*]")
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.sql.warehouse.dir", warehousePath)
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.ui.enabled", "false")
      .setAppName(appName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    val warehouseDir = new Directory(new File(warehousePath))
    if (warehouseDir.exists) {
      warehouseDir.deleteRecursively()
    }
  }

  def createDataFrame(schema: String, rows: Row*): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(rows), StructType.fromDDL(schema))
  }

  implicit class DateHelper(private val sc: StringContext) {
    def d(args: Any*): java.sql.Date = java.sql.Date.valueOf(sc.s(args: _*))
  }

  implicit class TimestampHelper(private val sc: StringContext) {
    def t(args: Any*): Timestamp = Timestamp.from(ZonedDateTime.parse(sc.s(args: _*)).toInstant)
  }

  def assertDataFrameDataInColumnsEqual(expected: DataFrame, actual: DataFrame, sortArrays: Boolean = true) = {
    val (expectedNormalized, actualNormalized) = if (sortArrays) {
      val expectedArrayCols  = expected.schema.collect { case StructField(name, _: ArrayType, _, _) => name }
      val actualArrayCols    = actual.schema.collect { case StructField(name, _: ArrayType, _, _) => name }
      val sortedArraysActual = actualArrayCols.foldLeft(actual)((df, cn) => df.withColumn(cn, array_sort(col(cn))))
      val sortedArraysExpected =
        expectedArrayCols.foldLeft(expected)((df, cn) => df.withColumn(cn, array_sort(col(cn))))
      (sortedArraysExpected, sortedArraysActual)
    } else {
      (expected, actual)
    }

    val expectedNamesOrder          = expected.schema.map(_.name)
    val actualSchemaInExpectedOrder = actual.schema.sortBy(f => expectedNamesOrder.indexOf(f.name))

    val actualInExpectedOrder  = actualNormalized.select(actualSchemaInExpectedOrder.map(_.name).map(col): _*)
    val expectedInInitialOrder = expectedNormalized.select(actualSchemaInExpectedOrder.map(_.name).map(col): _*)

    assertDataFrameDataEquals(expectedInInitialOrder, actualInExpectedOrder)
  }

  def argThatDataEqualsTo[T1, T2](stepName: String, expected: Dataset[T1]) =
    ArgumentMatchers.argThat((actual: Dataset[T2]) => {
      val trackingInfo = stepName
      assertDataFrameDataInColumnsEqual(actual.toDF(), expected.toDF())
      true
    })


  def assertCausedBy[T <: Throwable](exception: Throwable)(implicit causeClass: ClassTag[T]): T = {
    var currentException = exception
    var isCausedBy = false

    while (!isCausedBy) {
      currentException match {
        case null => fail(s"No `${causeClass.toString()}` exception was found in the cause chain for `$exception`")
        case e if causeClass.runtimeClass.isInstance(e) => isCausedBy = true
        case _ => currentException = currentException.getCause
      }
    }
    currentException.asInstanceOf[T]
  }
}
