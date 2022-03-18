package pb.dictionary.extraction.publish
//
//import org.apache.spark.sql.functions.lit
//import pb.dictionary.extraction.ApplicationManagedAreaTestBase
//import pb.dictionary.extraction.publish.sheets.{SheetsManualEnrichmentArea, UndefinedRow}
//import pb.dictionary.extraction.silver.DefinedText
//
//import java.io.File
//import java.sql.Timestamp
//import java.time.{ZonedDateTime, ZoneOffset}
//import scala.reflect.io.Directory
//
//class SheetsManualEnrichmentAreaTest extends ApplicationManagedAreaTestBase {
//  override val areaName: String = "manualEnrichment"
//
//  describe("upsert method") {
//    it("Should publish full snapshot of records that are not defined in silver and publish areas on each call") {
//      import spark.implicits._
//      val firstTimestamp    = t"2020-01-01T01:01:01Z"
//      val secondTimestamp   = t"2021-01-01T01:01:01Z"
//      val timestampProvider = changingTimestampProvider(firstTimestamp, secondTimestamp)
//      val area              = new SheetsManualEnrichmentArea(areaPath, timestampProvider)
//      val silverSnapshot = spark.createDataset(
//        Seq(
//          DefinedText(
//            "aaa",
//            Seq("aaa Book", "aaa Book2"),
//            3,
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            null,
//            null,
//            null,
//            null,
//            null,
//            null,
//            null,
//          ),
//          DefinedText(
//            "aa",
//            Seq("aa Book"),
//            1,
//            t"1999-01-01T01:01:01Z",
//            t"1999-01-01T01:01:01Z",
//            t"1999-01-01T01:01:01Z",
//            null,
//            null,
//            null,
//            null,
//            null,
//            null,
//            null,
//          ),
//          DefinedText(
//            "a",
//            Seq("a Book", "a Book2"),
//            2,
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            null,
//            null,
//            null,
//            null,
//            null,
//            null,
//            null,
//          ),
//          DefinedText(
//            "diehard",
//            Seq("diehard Book"),
//            1,
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            "diehard",
//            "ˈdʌɪhɑːd",
//            "noun",
//            "a person who strongly opposes change or who continues to support something in spite of opposition.",
//            Seq("a diehard Yankees fan"),
//            Seq("hard-line", "...", "blimp"),
//            Seq("modernizer")
//          ),
//          DefinedText(
//            "die hard",
//            Seq("die hard Book"),
//            1,
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            "diehard",
//            "ˈdʌɪhɑːd",
//            "noun",
//            "a person who strongly opposes change or who continues to support something in spite of opposition.",
//            Seq("a diehard Yankees fan"),
//            Seq("hard-line", "...", "blimp"),
//            Seq("modernizer")
//          )
//        )
//      )
//
//      val publishSnapshot = spark.createDataset(
//        Seq(
//          TestFinalPublishRecord(
//            1,
//            "a",
//            "A well-known word",
//            "a,an",
//            t"2101-01-01T01:01:01Z"
//          ),
//          TestFinalPublishRecord(
//            2,
//            "diehard",
//            "a person who strongly opposes change or who continues to support something in spite of opposition.",
//            "die hard,diehard",
//            t"2101-01-01T01:01:01Z"
//          ),
//        )
//      )
//
//      val firstActual = area.upsert(silverSnapshot, publishSnapshot)
//      val firstExpected = spark.createDataset(
//        Seq(
//          UndefinedRow(
//            "aaa",
//            "aaa Book, aaa Book2",
//            3,
//            "2100-01-01 01:01:01",
//            "2100-01-01 01:01:01",
//            firstTimestamp,
//          ),
//          UndefinedRow(
//            "aa",
//            "aa Book",
//            1,
//            "1999-01-01 01:01:01",
//            "1999-01-01 01:01:01",
//            firstTimestamp,
//          ),
//        )
//      )
//      assertDataFrameDataEquals(firstExpected.toDF(), firstActual.toDF())
//
//      val secondActual   = area.upsert(silverSnapshot, publishSnapshot)
//      val secondExpected = firstExpected.withColumn(UndefinedRow.UPDATED_AT, lit(secondTimestamp)).as[UndefinedRow]
//      assertDataFrameDataEquals(secondExpected.toDF(), secondActual.toDF())
//
//      val fullTableActual   = spark.table(area.fullTableName)
//      val fullTableExpected = firstExpected.unionByName(secondExpected)
//      assertDataFrameDataEquals(fullTableExpected.toDF(), fullTableActual.toDF())
//
//    }
//    it("Should handle multiline text") {
//      import spark.implicits._
//      val area = new SheetsManualEnrichmentArea(areaPath, testTimestampProvider)
//      val silverSnapshot = spark.createDataset(
//        Seq(
//          DefinedText(
//            "a\na",
//            Seq("a\na Book", "a a Book"),
//            1,
//            t"1999-01-01T01:01:01Z",
//            t"1999-01-01T01:01:01Z",
//            t"1999-01-01T01:01:01Z",
//            null,
//            null,
//            null,
//            null,
//            null,
//            null,
//            null,
//          ),
//          DefinedText(
//            "diehard",
//            Seq("diehard Book"),
//            1,
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            "diehard",
//            "ˈdʌɪhɑːd",
//            "noun",
//            "a person who strongly opposes change or who continues to support something in spite of opposition.",
//            Seq("a diehard Yankees fan"),
//            Seq("hard-line", "...", "blimp"),
//            Seq("modernizer")
//          ),
//          DefinedText(
//            "die hard",
//            Seq("die hard Book"),
//            1,
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            "diehard",
//            "ˈdʌɪhɑːd",
//            "noun",
//            "a person who strongly opposes change or who continues to support something in spite of opposition.",
//            Seq("a diehard Yankees fan"),
//            Seq("hard-line", "...", "blimp"),
//            Seq("modernizer")
//          ),
//          DefinedText(
//            "die\nhard",
//            Seq("die\nhard Book", "die hard Book"),
//            1,
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            t"2100-01-01T01:01:01Z",
//            null,
//            null,
//            null,
//            null,
//            null,
//            null,
//            null,
//          )
//        )
//      )
//
//      val publishSnapshot = spark.createDataset(
//        Seq(
//          TestFinalPublishRecord(
//            1,
//            "a\na",
//            "A weird word",
//            "aa,a\na,a a",
//            t"2101-01-01T01:01:01Z"
//          ),
//          TestFinalPublishRecord(
//            2,
//            "diehard",
//            "a person who strongly opposes change or who continues to support something in spite of opposition.",
//            "die hard,diehard",
//            t"2101-01-01T01:01:01Z"
//          ),
//        )
//      )
//
//      val actual = area.upsert(silverSnapshot, publishSnapshot)
//      val expected = spark.createDataset(
//        Seq(
//          UndefinedRow(
//            "die\nhard",
//            "die\nhard Book, die hard Book",
//            1,
//            "2100-01-01 01:01:01",
//            "2100-01-01 01:01:01",
//            testTimestamp,
//          )
//        )
//      )
//      assertDataFrameDataEquals(expected.toDF(), actual.toDF())
//
//      val actualTableState = spark.table(area.fullTableName)
//      assertDataFrameDataEquals(expected.toDF(), actualTableState.toDF())
//    }
//
//    it("should create a single file per publish") {
//      import spark.implicits._
//      val nUnknownRecords = 1000
//      val area            = new SheetsManualEnrichmentArea(areaPath, () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant))
//      val areaDir         = new Directory(new File(area.absoluteTablePath))
//
//      val silverSnapshot = spark
//        .createDataset(
//          (1 to nUnknownRecords).map(
//            i =>
//              DefinedText(
//                "a" * i,
//                Seq(s"${"a" * i} Book"),
//                1,
//                t"1999-01-01T01:01:01Z",
//                t"1999-01-01T01:01:01Z",
//                t"1999-01-01T01:01:01Z",
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null
//            )
//          ))
//        .repartition(10)
//      val publishSnapshot = spark.emptyDataset[TestFinalPublishRecord]
//
//      val actualFirst = area.upsert(silverSnapshot, publishSnapshot)
//      actualFirst.count() shouldBe nUnknownRecords
//      areaDir.deepFiles.count(_.name.endsWith(".csv")) shouldBe 1
//
//      val actualSecond = area.upsert(silverSnapshot, publishSnapshot)
//      actualSecond.count() shouldBe nUnknownRecords
//      spark.table(area.fullTableName).count() shouldBe nUnknownRecords * 2
//      areaDir.deepFiles.count(_.name.endsWith(".csv")) shouldBe 2
//    }
//  }
//}
//
//case class TestFinalPublishRecord(
//    id: Int,
//    normalizedText: String,
//    definition: String,
//    forms: String,
//    updatedAt: Timestamp
//) extends FinalPublishProduct
