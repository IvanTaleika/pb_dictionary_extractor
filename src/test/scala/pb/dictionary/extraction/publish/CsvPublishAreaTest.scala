package pb.dictionary.extraction.publish
//
//import org.mockito.Mockito
//import pb.dictionary.extraction.ApplicationManagedAreaTestBase
//import pb.dictionary.extraction.golden.DictionaryRecord
//
//import java.io.File
//import java.sql.Timestamp
//import java.time.{ZonedDateTime, ZoneOffset}
//import scala.reflect.io.Directory
//
//class CsvPublishAreaTest extends ApplicationManagedAreaTestBase {
//  override val areaName: String = "googleSheets"
//  import CsvRow._
//
//  describe("upsert method") {
//    it(
//      "Should publish everything from golden area as CSV after modifying data by join array columns, " +
//        "casting timestamps to string and assigning an id by the first text occurrence time if there " +
//        "are not data in google sheets yet") {
//      import spark.implicits._
//      val area = new CsvPublishArea(areaPath, testTimestampProvider)
//      val goldenSnapshot = spark.createDataset(
//        Seq(
//          DictionaryRecord(
//            "duck",
//            "noun",
//            "/dʌk/",
//            Seq("duck", "ducks"),
//            Seq("duck Book", "ducks Book"),
//            2,
//            t"1999-01-01T01:01:01Z",
//            t"2000-01-01T01:01:01Z",
//            "duck definition",
//            Seq("duck example"),
//            Seq("duck synonym"),
//            Seq("duck antonym"),
//            "duck translation",
//            Option(1d),
//            t"2010-01-01T01:01:01Z"
//          ),
//          DictionaryRecord(
//            "peevish",
//            "adjective",
//            "ˈpiːvɪʃ",
//            Seq("peevish"),
//            Seq("peevish Book"),
//            1,
//            t"2000-01-01T01:01:01Z",
//            t"2000-01-01T01:01:01Z",
//            "having or showing an irritable disposition.",
//            Seq("a thin peevish voice"),
//            Seq("irritable", "...", "miffy"),
//            Seq("affable", "easy-going"),
//            "peevish translation",
//            Option(0.25),
//            t"2010-01-01T01:01:01Z"
//          )
//        )
//      )
//
//      val actual = area.upsert(goldenSnapshot)
//      val expected = spark.createDataset(
//        Seq(
//          CsvRow(
//            1,
//            CsvRow.NewStatus,
//            "duck",
//            "noun",
//            "/dʌk/",
//            "duck, ducks",
//            "duck Book, ducks Book",
//            2,
//            "1999-01-01 01:01:01",
//            "2000-01-01 01:01:01",
//            "duck definition",
//            "duck example",
//            "duck synonym",
//            "duck antonym",
//            "duck translation",
//            "100.000000%",
//            testTimestamp
//          ),
//          CsvRow(
//            2,
//            CsvRow.NewStatus,
//            "peevish",
//            "adjective",
//            "ˈpiːvɪʃ",
//            "peevish",
//            "peevish Book",
//            1,
//            "2000-01-01 01:01:01",
//            "2000-01-01 01:01:01",
//            "having or showing an irritable disposition.",
//            "a thin peevish voice",
//            "irritable, ..., miffy",
//            "affable, easy-going",
//            "peevish translation",
//            "25.000000%",
//            testTimestamp
//          )
//        )
//      )
//      assertDataFrameDataEquals(expected.toDF(), actual.toDF())
//    }
//
//    it("Should publish should publish merged state if CsvPublishArea is not empty. ") {
//      import spark.implicits._
//      val area = new CsvPublishArea(areaPath, testTimestampProvider)
//      val testObj = Mockito.spy(area)
//      val goldenSnapshot = spark.createDataset(
//        Seq(
//          DictionaryRecord(
//            "duck",
//            "noun",
//            "/dʌk/",
//            Seq("duck", "ducks"),
//            Seq("duck Book", "ducks Book"),
//            2,
//            t"1999-01-01T01:01:01Z",
//            t"2000-01-01T01:01:01Z",
//            "duck definition",
//            Seq("duck example"),
//            Seq("duck synonym", "ducks synonym"),
//            Seq("duck antonym"),
//            "duck translation",
//            Option(1d),
//            t"2010-01-01T01:01:01Z"
//          ),
//          DictionaryRecord(
//            "die hard",
//            null,
//            null,
//            Seq("die hard"),
//            Seq("die hard Book"),
//            1,
//            t"1999-01-01T01:01:02Z",
//            t"1999-01-01T01:01:02Z",
//            "disappear or change very slowly.",
//            Seq.empty,
//            Seq.empty,
//            Seq.empty,
//            null,
//            Option(1d),
//            t"2020-01-01T00:00:00Z",
//          ),
//          DictionaryRecord(
//            "diehard",
//            "noun",
//            "ˈdʌɪhɑːd",
//            Seq("die hard", "diehard"),
//            Seq("die hard Book"),
//            2,
//            t"1999-01-01T01:01:02Z",
//            t"2000-01-01T01:01:01Z",
//            "a person who strongly opposes change or who continues to support something in spite of opposition.",
//            Seq("a diehard Yankees fan"),
//            Seq("hard-line", "blimp"),
//            Seq("modernizer"),
//            "diehard translation",
//            Option(1d),
//            t"2020-01-01T00:00:00Z",
//          )
//        )
//      )
//
//      val publishedSheet = spark.createDataset(
//        Seq(
//          CsvRow(
//            1,
//            LearnedStatus,
//            "duck",
//            "noun",
//            "/dʌk/",
//            "duck, ducks",
//            "duck Book, duck manual source",
//            1,
//            "1999-01-01 01:01:01",
//            "1999-01-01 01:01:01",
//            "duck definition",
//            "duck example, duck manual example",
//            ".",
//            null,
//            "duck translation, duck manual translation",
//            "50.000000%",
//            t"2019-01-01T01:01:01Z"
//          ),
//          CsvRow(
//            2,
//            InProgressStatus,
//            "die hard",
//            "phrase",
//            "ˈdʌɪ hɑːd",
//            "die hard",
//            "die hard Book",
//            1,
//            " ",
//            null,
//            "disappear or change very slowly.",
//            "die hard example",
//            "die hard synonym",
//            "die hard antonym",
//            "die hard translation",
//            "100.000000%",
//            t"2019-01-01T01:01:01Z"
//          ),
//          CsvRow(
//            4,
//            CsvRow.NewStatus,
//            "peevish",
//            "adjective",
//            "ˈpiːvɪʃ",
//            "peevish",
//            "peevish Book",
//            1,
//            "2000-01-01 01:01:01",
//            "2000-01-01 01:01:01",
//            "having or showing an irritable disposition.",
//            "a thin peevish voice",
//            "irritable, ..., miffy",
//            "affable, easy-going",
//            "peevish translation",
//            "25.000000%",
//            t"2019-01-01T01:01:01Z"
//          )
//        )
//      )
//
//      Mockito.doReturn(publishedSheet, Nil: _*).when(testObj).snapshot
//
//      testObj.upsert(goldenSnapshot)
//      val actual = spark.table(area.fullTableName)
//      val expected = spark.createDataset(
//        Seq(
//          CsvRow(
//            1,
//            LearnedStatus,
//            "duck",
//            "noun",
//            "/dʌk/",
//            "duck, ducks",
//            "duck Book, duck manual source, ducks Book",
//            2,
//            "1999-01-01 01:01:01",
//            "2000-01-01 01:01:01",
//            "duck definition",
//            "duck example, duck manual example",
//            "., duck synonym, ducks synonym",
//            "duck antonym",
//            "duck translation, duck manual translation",
//            "50.000000%",
//            testTimestamp
//          ),
//          CsvRow(
//            2,
//            InProgressStatus,
//            "die hard",
//            "phrase",
//            "ˈdʌɪ hɑːd",
//            "die hard",
//            "die hard Book",
//            1,
//            "1999-01-01 01:01:02",
//            "1999-01-01 01:01:02",
//            "disappear or change very slowly.",
//            "die hard example",
//            "die hard synonym",
//            "die hard antonym",
//            "die hard translation",
//            "100.000000%",
//            testTimestamp
//          ),
//          CsvRow(
//            4,
//            CsvRow.NewStatus,
//            "peevish",
//            "adjective",
//            "ˈpiːvɪʃ",
//            "peevish",
//            "peevish Book",
//            1,
//            "2000-01-01 01:01:01",
//            "2000-01-01 01:01:01",
//            "having or showing an irritable disposition.",
//            "a thin peevish voice",
//            "irritable, ..., miffy",
//            "affable, easy-going",
//            "peevish translation",
//            "25.000000%",
//            testTimestamp
//          ),
//          CsvRow(
//            5,
//            CsvRow.NewStatus,
//            "diehard",
//            "noun",
//            "ˈdʌɪhɑːd",
//            "die hard, diehard",
//            "die hard Book",
//            2,
//            "1999-01-01 01:01:02",
//            "2000-01-01 01:01:01",
//            "a person who strongly opposes change or who continues to support something in spite of opposition.",
//            "a diehard Yankees fan",
//            "hard-line, blimp",
//            "modernizer",
//            "diehard translation",
//            "100.000000%",
//            testTimestamp
//          )
//        )
//      )
//      assertDataFrameDataEquals(expected.toDF(), actual.toDF())
//    }
//
//    it("should create a single file per publish") {
//      import spark.implicits._
//      val nUnknownRecords = 1000
//      val area            = new CsvPublishArea(areaPath, () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant))
//      val areaDir         = new Directory(new File(area.absoluteTablePath))
//
//      val goldenSnapshot = spark
//        .createDataset(
//          (1 to nUnknownRecords).map(
//            i =>
//              DictionaryRecord(
//                "a" * i,
//                "adjective",
//                "ˈpiːvɪʃ",
//                Seq("a" * i),
//                Seq(s"${"a" * i} Book"),
//                1,
//                t"2000-01-01T01:01:01Z",
//                t"2000-01-01T01:01:01Z",
//                s"${"a" * i} definition",
//                Seq(s"${"a" * i} example"),
//                Seq.empty,
//                Seq.empty,
//                s"${"a" * i} translation",
//                Option(0.25),
//                t"2010-01-01T01:01:01Z"
//            )
//          ))
//        .repartition(10)
//
//      val actualFirst = area.upsert(goldenSnapshot)
//      actualFirst.count() shouldBe nUnknownRecords
//      areaDir.deepFiles.count(_.name.endsWith(".csv")) shouldBe 1
//
//      val actualSecond = area.upsert(goldenSnapshot)
//      actualSecond.count() shouldBe nUnknownRecords
//      spark.table(area.fullTableName).count() shouldBe nUnknownRecords * 2
//      areaDir.deepFiles.count(_.name.endsWith(".csv")) shouldBe 2
//    }
//  }
//}
