package pb.dictionary.extraction

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import grizzled.slf4j.Logger
import org.apache.http.{HttpHost, HttpRequest}
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.protocol.HttpContext
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkConf
import org.mockito.ArgumentMatchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}

import java.io.File
import java.sql.Timestamp
import java.time.ZonedDateTime
import scala.reflect.io.Directory

abstract class TestBase
    extends FunSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
    with MockFactory
    with DataFrameSuiteBase {
  val warehousePath = "target/spark-warehouse"
  protected val logger = Logger(getClass)

  override def conf: SparkConf =
    super.conf
      .setMaster("local[1]")
//      .setMaster("local[*]")
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.sql.warehouse.dir", warehousePath)
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // TODO: remove spark UI
      .set("spark.ui.enabled", "true")
      .setAppName("pb_dictionary_extractor_tests")

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

  def argThatDataEqualsTo[T1, T2](stepName: String, expected: Dataset[T1]) =
    ArgumentMatchers.argThat((actual: Dataset[T2]) => {
      val trackingInfo = stepName
      assertDataFrameNoOrderEquals(actual.toDF(), expected.toDF())
      true
    })
}
