package pb.dictionary.extraction

import java.io.File
import java.sql.Timestamp
import scala.reflect.io.Directory

abstract class ApplicationManagedAreaTestBase extends TestBase {
  val areaName: String
  def dbName                                 = s"${areaName}AreaTest"
  def dbRoot                                 = s"target/$dbName"
  def areaPath                               = s"$dbRoot/$areaName"
  def testTimestamp                          = t"2021-01-01T01:01:01Z"
  def testTimestampProvider: () => Timestamp = () => testTimestamp

  def changingTimestampProvider(tss: Timestamp*): () => Timestamp = {
    val provider = mockFunction[Timestamp]
    tss.foreach { ts =>
      provider.expects().returns(ts).once()
    }
    provider
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    purgeTestDb()
  }

  def purgeTestDb(): Unit = {
    spark.sql(s"drop table if exists $dbName.$areaName")
    val dir = new Directory(new File(dbRoot))
    if (dir.exists) {
      dir.deleteRecursively()
    }
    dir.createDirectory()
  }
}
