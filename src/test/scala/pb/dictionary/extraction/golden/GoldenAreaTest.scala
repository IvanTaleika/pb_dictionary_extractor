package pb.dictionary.extraction.golden

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalamock.function.FunctionAdapter1
import pb.dictionary.extraction.ApplicationManagedAreaTestBase
import pb.dictionary.extraction.silver.DefinedText


class GoldenAreaTest extends ApplicationManagedAreaTestBase {
  import DictionaryRecord._
  var preMergeSchema: StructType   = _
  var preMergeSchemaDdl: String    = _
  var fromSilverSchema: StructType = _
  var fromSilverSchemaDdl: String  = _
  override def beforeAll() = {
    super.beforeAll()
    preMergeSchema    = StructType(Encoders.product[DictionaryRecord].schema.filterNot(_.name == UPDATED_AT))
    preMergeSchemaDdl = preMergeSchema.toDDL
    fromSilverSchema = StructType(
      preMergeSchema.filterNot {
        val enrichedAttributesSet = enrichedAttributes.toSet
        dt =>
          enrichedAttributesSet.contains(dt.name)
      }
    )
    fromSilverSchemaDdl = fromSilverSchema.toDDL
  }

  override val areaName: String = "golden"
  describe("update method") {
    it(
      "Should select all updated definitions, send new definition through translate and usage APIs " +
        "and execute merge method") {
      import spark.implicits._
      val dictionaryTranslationApi = Mockito.mock(classOf[DictionaryTranslationApi])
      val usageFrequencyApi        = Mockito.mock(classOf[UsageFrequencyApi])
      val area                     = new GoldenArea(areaPath, dictionaryTranslationApi, usageFrequencyApi, testTimestampProvider)
      val testObj                  = Mockito.spy(area)
      val silverSnapshot = {
        val validUpdates = spark.createDataset(
          Seq(
            DefinedText(
              "diehard",
              Seq("diehard Book"),
              1,
              t"2000-01-01T01:01:01Z",
              t"2000-01-01T01:01:01Z",
              t"2022-01-01T01:01:01Z",
              "diehard",
              "ˈdʌɪhɑːd",
              "noun",
              "a person who strongly opposes change or who continues to support something in spite of opposition.",
              Seq("a diehard Yankees fan"),
              Seq("hard-line", "...", "blimp"),
              Seq("modernizer")
            ),
            DefinedText(
              "ducks",
              Seq("ducks Book"),
              1,
              t"2000-01-01T01:01:01Z",
              t"2000-01-01T01:01:01Z",
              t"2022-01-01T01:01:01Z",
              "duck",
              "/dʌk/",
              "noun",
              "duck definition",
              Seq("duck example"),
              Seq("duck synonym"),
              Seq("duck antonym"),
            ),
            DefinedText(
              "peevish",
              Seq("peevish Book"),
              1,
              t"2000-01-01T01:01:01Z",
              t"2000-01-01T01:01:01Z",
              t"2022-01-01T01:01:01Z",
              "peevish",
              "ˈpiːvɪʃ",
              "adjective",
              "having or showing an irritable disposition.",
              Seq("a thin peevish voice"),
              Seq("irritable", "...", "miffy"),
              Seq("affable", "easy-going")
            )
          )
        )
        val invalidUpdates = spark.createDataset(
          Seq(
            DefinedText(
              "aaa",
              Seq("aaa Book"),
              1,
              t"2000-01-01T01:01:01Z",
              t"2000-01-01T01:01:01Z",
              t"2022-01-01T01:01:01Z",
              null,
              null,
              null,
              null,
              null,
              null,
              null,
            )
          )
        )
        val silverUpdates = validUpdates.unionByName(invalidUpdates)
        val silverUpdatedDefinitions = spark.createDataset(
          Seq(
            DefinedText(
              "die hard",
              Seq("die hard Book"),
              1,
              t"1999-01-01T01:01:01Z",
              t"1999-01-01T01:01:01Z",
              t"1999-01-01T01:01:01Z",
              "diehard",
              "ˈdʌɪhɑːd",
              "noun",
              "a person who strongly opposes change or who continues to support something in spite of opposition.",
              Seq("a diehard Yankees fan"),
              Seq("hard-line", "...", "blimp"),
              Seq("modernizer")
            ),
            DefinedText(
              "duck",
              Seq("duck Book"),
              1,
              t"1999-01-01T01:01:01Z",
              t"1999-01-01T01:01:01Z",
              t"1999-01-01T01:01:01Z",
              "duck",
              "/dʌk/",
              "noun",
              "duck definition",
              Seq("duck example"),
              Seq("duck synonym"),
              Seq("duck antonym"),
            )
          )
        )
        val silverUnchangedRecords = spark.createDataset(
          Seq(
            DefinedText(
              "die hard",
              Seq("die hard Book"),
              1,
              t"1999-01-01T01:01:01Z",
              t"1999-01-01T01:01:01Z",
              t"1999-01-01T01:01:01Z",
              "die hard",
              null,
              null,
              "disappear or change very slowly.",
              Seq("old habits die hard"),
              Seq.empty,
              Seq.empty
            ),
            DefinedText(
              "bbb",
              Seq("aaa Book"),
              1,
              t"1999-01-01T01:01:01Z",
              t"1999-01-01T01:01:01Z",
              t"1999-01-01T01:01:01Z",
              null,
              null,
              null,
              null,
              null,
              null,
              null,
            )
          )
        )
        silverUpdates.unionByName(silverUpdatedDefinitions).unionByName(silverUnchangedRecords)
      }

      val goldenSnapshot = spark.createDataset(
        Seq(
          DictionaryRecord(
            "die hard",
            null,
            null,
            Seq("die hard"),
            Seq("die hard Book"),
            1,
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            "disappear or change very slowly.",
            Seq("old habits die hard"),
            Seq.empty,
            Seq.empty,
            "die hard translation",
            Option(1d),
            t"2020-01-01T00:00:00Z",
          ),
          DictionaryRecord(
            "diehard",
            "noun",
            "ˈdʌɪhɑːd",
            Seq("die hard"),
            Seq("die hard Book"),
            2,
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            "a person who strongly opposes change or who continues to support something in spite of opposition.",
            Seq("a diehard Yankees fan"),
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer"),
            "diehard translation",
            Option(1d),
            t"2020-01-01T00:00:00Z",
          )
        )
      )
      Mockito.doReturn(goldenSnapshot, Nil: _*).when(testObj).snapshot

      val expectedNewEntries = createDataFrame(
        fromSilverSchemaDdl,
        Row(
          "duck",
          "noun",
          "/dʌk/",
          Seq("duck", "ducks"),
          Seq("duck Book", "ducks Book"),
          2,
          t"1999-01-01T01:01:01Z",
          t"2000-01-01T01:01:01Z",
          "duck definition",
          Seq("duck example"),
          Seq("duck synonym"),
          Seq("duck antonym"),
        ),
        Row(
          "peevish",
          "adjective",
          "ˈpiːvɪʃ",
          Seq("peevish"),
          Seq("peevish Book"),
          1,
          t"2000-01-01T01:01:01Z",
          t"2000-01-01T01:01:01Z",
          "having or showing an irritable disposition.",
          Seq("a thin peevish voice"),
          Seq("irritable", "...", "miffy"),
          Seq("affable", "easy-going"),
        )
      )

      val expectedAreaUpdates = spark.createDataset(
        Seq(
          DictionaryRecord(
            "duck",
            "noun",
            "/dʌk/",
            Seq("duck", "ducks"),
            Seq("duck Book", "ducks Book"),
            2,
            t"1999-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "duck definition",
            Seq("duck example"),
            Seq("duck synonym"),
            Seq("duck antonym"),
            "duck translated",
            Option(1d),
            testTimestamp
          ),
          DictionaryRecord(
            "peevish",
            "adjective",
            "ˈpiːvɪʃ",
            Seq("peevish"),
            Seq("peevish Book"),
            1,
            t"2000-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "having or showing an irritable disposition.",
            Seq("a thin peevish voice"),
            Seq("irritable", "...", "miffy"),
            Seq("affable", "easy-going"),
            "peevish translated",
            Option(1d),
            testTimestamp
          ),
          DictionaryRecord(
            "diehard",
            "noun",
            "ˈdʌɪhɑːd",
            Seq("diehard", "die hard"),
            Seq("diehard Book", "die hard Book"),
            2,
            t"1999-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "a person who strongly opposes change or who continues to support something in spite of opposition.",
            Seq("a diehard Yankees fan"),
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer"),
            null,
            Option.empty,
            testTimestamp
          )
        )
      )

      val translatedDf = expectedNewEntries.withColumn(TRANSLATION, concat(col(NORMALIZED_TEXT), lit(" translated")))
      Mockito.when(dictionaryTranslationApi.translate(ArgumentMatchers.any())).thenReturn(translatedDf)

      val usageDf = translatedDf.withColumn(USAGE, lit(1d))
      Mockito.when(usageFrequencyApi.findUsageFrequency(ArgumentMatchers.any())).thenReturn(usageDf)

      val expected = Mockito.mock(classOf[Dataset[DictionaryRecord]])
      Mockito.doReturn(expected, Nil: _*).when(testObj).updateArea(ArgumentMatchers.any())

      val actual = testObj.upsert(silverSnapshot)
      actual shouldBe expected

      Mockito
        .verify(dictionaryTranslationApi, Mockito.only())
        .translate(argThatDataEqualsTo("translate", expectedNewEntries))
      Mockito
        .verify(usageFrequencyApi, Mockito.only())
        .findUsageFrequency(argThatDataEqualsTo("findUsageFrequency", translatedDf))
      Mockito
        .verify(testObj, Mockito.atMostOnce())
        .updateArea(argThatDataEqualsTo("updateArea", expectedAreaUpdates))
    }

    it("should ignore silver records with `null`s in the golden area PK columns") {
      import spark.implicits._
      val dictionaryTranslationApi = mock[DictionaryTranslationApi]
      (dictionaryTranslationApi.translate _)
        .expects(new FunctionAdapter1[DataFrame, Boolean](_.isEmpty))
        .onCall { df: DataFrame =>
          df.withColumn(TRANSLATION, lit("N/A"))
        }
        .once()

      val usageFrequencyApi = mock[UsageFrequencyApi]
      (usageFrequencyApi.findUsageFrequency _)
        .expects(new FunctionAdapter1[DataFrame, Boolean](_.isEmpty))
        .onCall { df: DataFrame =>
          df.withColumn(USAGE, lit(-1d))
        }
        .once()

      val area    = new GoldenArea(areaPath, dictionaryTranslationApi, usageFrequencyApi, testTimestampProvider)
      val testObj = Mockito.spy(area)
      val silverSnapshot = spark.createDataset(
        Seq(
          DefinedText(
            "aaa",
            Seq("aaa Book"),
            1,
            t"2000-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            t"2022-01-01T01:01:01Z",
            "aaa",
            null,
            null,
            null,
            null,
            null,
            null,
          ),
          DefinedText(
            "bbb",
            Seq("aaa Book"),
            1,
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            null,
            "random bbb string",
            null,
            null,
            null,
            null,
            null,
          ),
          DefinedText(
            "ccc",
            Seq("ccc Book"),
            2,
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
          )
        )
      )
      val actual   = testObj.upsert(silverSnapshot)
      val expected = spark.emptyDataset[DictionaryRecord]
    }
  }

  describe("updateArea") {
    it("Should insert everything if golden area is empty") {
      import spark.implicits._
      val area = new GoldenArea(areaPath, null, null, null)
      val updates = spark.createDataset(
        Seq(
          DictionaryRecord(
            "duck",
            "noun",
            "/dʌk/",
            Seq("duck", "ducks"),
            Seq("duck Book", "ducks Book"),
            2,
            t"1999-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "duck definition",
            Seq("duck example"),
            Seq("duck synonym"),
            Seq("duck antonym"),
            "duck translation",
            Option(1d),
            testTimestamp
          ),
          DictionaryRecord(
            "peevish",
            "adjective",
            "ˈpiːvɪʃ",
            Seq("peevish"),
            Seq("peevish Book"),
            1,
            t"2000-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "having or showing an irritable disposition.",
            Seq("a thin peevish voice"),
            Seq("irritable", "...", "miffy"),
            Seq("affable", "easy-going"),
            "peevish translation",
            Option(1d),
            testTimestamp
          ),
          DictionaryRecord(
            "diehard",
            "noun",
            "ˈdʌɪhɑːd",
            Seq("diehard", "die hard"),
            Seq("diehard Book", "die hard Book"),
            2,
            t"1999-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "a person who strongly opposes change or who continues to support something in spite of opposition.",
            Seq("a diehard Yankees fan"),
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer"),
            null,
            Option.empty,
            testTimestamp
          )
        ))
      val actual = area.updateArea(updates)
      assertDataFrameDataEquals(updates.toDF(), actual.toDF())
    }

    it("Should insert new definitions and update attributes for existing definitions") {
      import spark.implicits._
      val firstTimestamp  = t"2021-01-01T01:01:01Z"
      val secondTimestamp = t"2021-01-01T01:01:02Z"
      val area            = new GoldenArea(areaPath, null, null, null)
      val initialState = spark.createDataset(
        Seq(
          DictionaryRecord(
            "duck",
            "noun",
            "/dʌk/",
            Seq("duck"),
            Seq("duck Book"),
            1,
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            "duck definition",
            Seq("duck example"),
            Seq("duck synonym"),
            Seq("duck antonym"),
            "duck translation",
            Option(1d),
            firstTimestamp
          ),
          DictionaryRecord(
            "diehard",
            "noun",
            "ˈdʌɪhɑːd",
            Seq("diehard"),
            Seq("diehard Book"),
            1,
            t"2000-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "a person who strongly opposes change or who continues to support something in spite of opposition.",
            Seq("a diehard Yankees fan"),
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer"),
            "diehard translation",
            Option(1d),
            firstTimestamp
          )
        )
      )
      area.updateArea(initialState)

      val updates = spark.createDataset(
        Seq(
          DictionaryRecord(
            "duck",
            "noun",
            "/dʌk/",
            Seq("duck", "ducks"),
            Seq("duck Book", "ducks Book"),
            2,
            t"1999-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "duck definition",
            Seq("duck example"),
            Seq("duck synonym"),
            Seq("duck antonym"),
            null,
            Option.empty,
            secondTimestamp
          ),
          DictionaryRecord(
            "peevish",
            "adjective",
            "ˈpiːvɪʃ",
            Seq("peevish"),
            Seq("peevish Book"),
            1,
            t"2000-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "having or showing an irritable disposition.",
            Seq("a thin peevish voice"),
            Seq("irritable", "...", "miffy"),
            Seq("affable", "easy-going"),
            "peevish translation",
            Option(1d),
            secondTimestamp
          )
        ))

      val actual = area.updateArea(updates)
      val expectedUpdates = spark.createDataset(
        Seq(
          DictionaryRecord(
            "duck",
            "noun",
            "/dʌk/",
            Seq("duck", "ducks"),
            Seq("duck Book", "ducks Book"),
            2,
            t"1999-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "duck definition",
            Seq("duck example"),
            Seq("duck synonym"),
            Seq("duck antonym"),
            "duck translation",
            Option(1d),
            secondTimestamp
          ),
          DictionaryRecord(
            "peevish",
            "adjective",
            "ˈpiːvɪʃ",
            Seq("peevish"),
            Seq("peevish Book"),
            1,
            t"2000-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "having or showing an irritable disposition.",
            Seq("a thin peevish voice"),
            Seq("irritable", "...", "miffy"),
            Seq("affable", "easy-going"),
            "peevish translation",
            Option(1d),
            secondTimestamp
          )
        )
      )
      val expectedUnchangedRecords = spark.createDataset(
        Seq(
          DictionaryRecord(
            "diehard",
            "noun",
            "ˈdʌɪhɑːd",
            Seq("diehard"),
            Seq("diehard Book"),
            1,
            t"2000-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "a person who strongly opposes change or who continues to support something in spite of opposition.",
            Seq("a diehard Yankees fan"),
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer"),
            "diehard translation",
            Option(1d),
            firstTimestamp
          )
        )
      )
      val expected = expectedUpdates.unionByName(expectedUnchangedRecords)
      assertDataFrameDataEquals(expected.toDF(), actual.toDF())
    }

    it("Should do nothing if there are no valid changes in SilverArea") {
      import spark.implicits._
      val area            = new GoldenArea(areaPath, null, null, null)
      val initialState = spark.createDataset(
        Seq(
          DictionaryRecord(
            "duck",
            "noun",
            "/dʌk/",
            Seq("duck"),
            Seq("duck Book"),
            1,
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            "duck definition",
            Seq("duck example"),
            Seq("duck synonym"),
            Seq("duck antonym"),
            "duck translation",
            Option(1d),
            testTimestamp
          ),
          DictionaryRecord(
            "diehard",
            "noun",
            "ˈdʌɪhɑːd",
            Seq("diehard"),
            Seq("diehard Book"),
            1,
            t"2000-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            "a person who strongly opposes change or who continues to support something in spite of opposition.",
            Seq("a diehard Yankees fan"),
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer"),
            "diehard translation",
            Option(1d),
            testTimestamp
          )
        ))
      area.updateArea(initialState)
      val expected = spark.table(area.fullTableName)
      val updates  = spark.emptyDataset[DictionaryRecord]
      val actual   = area.updateArea(updates)
      assertDataFrameDataEquals(expected.toDF(), actual.toDF())
    }
  }

  describe("fromSilver method") {
    it(
      s"Should select the latest definition (silver) attributes ordering by update time and merge " +
        s"highlighting (bronze) info grouping by $NORMALIZED_TEXT and $DEFINITION") {
      import spark.implicits._
      val area = new GoldenArea(areaPath, null, null)
      val definedFords = spark.createDataset(
        Seq(
          DefinedText(
            "ducks",
            Seq("ducks Book"),
            2,
            t"2000-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            t"2020-01-01T01:01:01Z",
            "duck",
            "/dʌk/",
            "noun",
            "duck definition",
            Seq.empty,
            Seq.empty,
            Seq.empty,
          ),
          DefinedText(
            "duck",
            Seq("duck Book"),
            1,
            t"2001-01-01T01:01:01Z",
            t"2001-01-01T01:01:01Z",
            t"2021-01-01T01:01:01Z",
            "duck",
            "/dʌk/",
            "noun",
            "duck definition",
            Seq("duck example"),
            Seq("duck synonym"),
            Seq("duck antonym"),
          ),
          DefinedText(
            "duck",
            Seq("duck Book"),
            2,
            t"2001-01-01T01:01:01Z",
            t"2001-01-01T01:01:01Z",
            t"2021-01-01T01:01:01Z",
            "duck",
            "/dʌk/",
            "noun",
            "duck alternative definition",
            Seq("duck example"),
            Seq("duck synonym"),
            Seq("duck antonym"),
          ),
          DefinedText(
            "ducking",
            Seq("duck verb Book"),
            1,
            t"2000-01-01T01:01:01Z",
            t"2000-01-01T01:01:01Z",
            t"2020-01-01T01:01:01Z",
            "duck",
            "/dʌk/",
            "verb",
            "lower the head or the body quickly to avoid a blow or missile or so as not to be seen.",
            Seq("duck example definition"),
            Seq("bob down"),
            Seq("straighten up"),
          ),
          DefinedText(
            "duck",
            Seq("duck verb Book"),
            1,
            t"2001-01-01T01:01:01Z",
            t"2001-01-01T01:01:01Z",
            t"2021-01-01T01:01:01Z",
            "duck",
            "/dʌk/",
            "verb",
            "lower the head or the body quickly to avoid a blow or missile or so as not to be seen.",
            Seq("spectators ducked for cover"),
            Seq("bob down", "bend (down)"),
            Seq("straighten up", "stand"),
          ),
          DefinedText(
            "ducked",
            Seq("duck verb Book 2"),
            1,
            t"2002-01-01T01:01:01Z",
            t"2002-01-01T01:01:01Z",
            t"2022-01-01T01:01:01Z",
            "duck",
            null,
            null,
            "lower the head or the body quickly to avoid a blow or missile or so as not to be seen.",
            Seq.empty,
            Seq.empty,
            Seq.empty,
          )
        )
      )
      val actual = area.fromSilver(definedFords)
      val expected = createDataFrame(
        fromSilverSchemaDdl,
        Row(
          "duck",
          "noun",
          "/dʌk/",
          Seq("duck", "ducks"),
          Seq("duck Book", "ducks Book"),
          3,
          t"2000-01-01T01:01:01Z",
          t"2001-01-01T01:01:01Z",
          "duck definition",
          Seq("duck example"),
          Seq("duck synonym"),
          Seq("duck antonym"),
        ),
        Row(
          "duck",
          "noun",
          "/dʌk/",
          Seq("duck"),
          Seq("duck Book"),
          2,
          t"2001-01-01T01:01:01Z",
          t"2001-01-01T01:01:01Z",
          "duck alternative definition",
          Seq("duck example"),
          Seq("duck synonym"),
          Seq("duck antonym"),
        ),
        Row(
          "duck",
          null,
          null,
          Seq("duck", "ducked", "ducking"),
          Seq("duck verb Book", "duck verb Book 2"),
          3,
          t"2000-01-01T01:01:01Z",
          t"2002-01-01T01:01:01Z",
          "lower the head or the body quickly to avoid a blow or missile or so as not to be seen.",
          Seq.empty,
          Seq.empty,
          Seq.empty,
        ),
      )
      val sortedActual = actual.withColumn(BOOKS, array_sort(col(BOOKS))).withColumn(FORMS, array_sort(col(FORMS)))
      assertDataFrameDataEquals(expected, sortedActual)
    }
  }

}
