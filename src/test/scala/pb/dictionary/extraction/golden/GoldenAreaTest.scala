package pb.dictionary.extraction.golden

import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.mockito.Mockito
import pb.dictionary.extraction.ApplicationManagedAreaTestBase
import pb.dictionary.extraction.silver.DefinedText

// TODO: test that nulls do not propagate in the area
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
      val area                     = new GoldenArea(areaPath, dictionaryTranslationApi, usageFrequencyApi)
      val testObj                  = Mockito.spy(area)
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
            "a diehard Yankees fan",
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
            "duck example",
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
            "a thin peevish voice",
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
            "a diehard Yankees fan",
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer")
          ),
          DefinedText(
            "duck",
            Seq("duck Book"),
            1,
            t"1999-10-10T10:10:10Z",
            t"1999-10-10T10:10:10Z",
            t"1999-10-10T10:10:10Z",
            "duck",
            "/dʌk/",
            "noun",
            "duck definition",
            "duck example",
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
            "old habits die hard",
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
      val silverSnapshot = silverUpdates.unionByName(silverUpdatedDefinitions).unionByName(silverUnchangedRecords)

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
            "old habits die hard",
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
            "a diehard Yankees fan",
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer"),
            "diehard translation",
            Option(1d),
            t"2020-01-01T00:00:00Z",
          )
        )
      )

      Mockito.doReturn(goldenSnapshot, Nil: _*).when(testObj).snapshot

      val expectedNewDefinitions = createDataFrame(
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
          "duck example",
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
          "a thin peevish voice",
          Seq("irritable", "...", "miffy"),
          Seq("affable", "easy-going"),
        )
      )

      val expectedDefinitionUpdates = createDataFrame(
        fromSilverSchemaDdl,
        Row(
          "diehard",
          "noun",
          "ˈdʌɪhɑːd",
          Seq("diehard", "die hard"),
          Seq("diehard Book", "die hard Book"),
          2,
          t"1999-01-01T01:01:01Z",
          t"2000-01-01T01:01:01Z",
          "a person who strongly opposes change or who continues to support something in spite of opposition.",
          "a diehard Yankees fan",
          Seq("hard-line", "...", "blimp"),
          Seq("modernizer"),
        )
      )

      val fromSilverDf = expectedNewDefinitions.unionByName(expectedDefinitionUpdates)

      Mockito
        .doReturn(fromSilverDf, Nil: _*)
        .when(testObj)
        .fromSilver(argThatDataEqualsTo("fromSilver", validUpdates.unionByName(silverUpdatedDefinitions)))

      val translatedDf =
        expectedNewDefinitions.withColumn(TRANSLATION, concat(col(NORMALIZED_TEXT), lit(" translated")))
      Mockito
        .when(dictionaryTranslationApi.translate(argThatDataEqualsTo("translate", expectedNewDefinitions)))
        .thenReturn(translatedDf)

      val usageDf = translatedDf.withColumn(USAGE, lit(1d))
      Mockito
        .when(usageFrequencyApi.findUsageFrequency(argThatDataEqualsTo("findUsageFrequency", translatedDf)))
        .thenReturn(usageDf)

      val expected = Mockito.mock(classOf[Dataset[DictionaryRecord]])
      Mockito
        .doReturn(expected, Nil: _*)
        .when(testObj)
        .updateArea(argThatDataEqualsTo("updateArea1", expectedDefinitionUpdates))(argThatDataEqualsTo("updateArea2", usageDf))

      val actual = testObj.upsert(silverSnapshot)
      actual shouldBe expected
    }
  }

  describe("updateArea") {
    it("Should insert everything if golden area is empty") {
      import spark.implicits._
      val area = new GoldenArea(areaPath, null, null, testTimestampProvider)
      val updates = createDataFrame(
        preMergeSchemaDdl,
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
          "duck example",
          Seq("duck synonym"),
          Seq("duck antonym"),
          "duck translation",
          1d
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
          "a thin peevish voice",
          Seq("irritable", "...", "miffy"),
          Seq("affable", "easy-going"),
          "peevish translation",
          1d
        ),
        Row(
          "diehard",
          "noun",
          "ˈdʌɪhɑːd",
          Seq("diehard", "die hard"),
          Seq("diehard Book", "die hard Book"),
          2,
          t"1999-01-01T01:01:01Z",
          t"2000-01-01T01:01:01Z",
          "a person who strongly opposes change or who continues to support something in spite of opposition.",
          "a diehard Yankees fan",
          Seq("hard-line", "...", "blimp"),
          Seq("modernizer"),
          null,
          null
        )
      )
      val actual   = area.updateArea(updates)(updates)
      val expected = updates.withColumn(UPDATED_AT, lit(testTimestamp)).as[DictionaryRecord]
      assertDataFrameDataEquals(expected.toDF(), actual.toDF())
    }

    it("Should insert new definitions and update attributes for existing definitions") {
      import spark.implicits._
      val firstTimestamp    = t"2021-01-01T01:01:01Z"
      val secondTimestamp   = t"2021-01-01T01:01:02Z"
      val timestampProvider = changingTimestampProvider(firstTimestamp, secondTimestamp)
      val area              = new GoldenArea(areaPath, null, null, timestampProvider)
      val initialState = createDataFrame(
        preMergeSchemaDdl,
        Row(
          "duck",
          "noun",
          "/dʌk/",
          Seq("duck"),
          Seq("duck Book"),
          1,
          t"1999-01-01T01:01:01Z",
          t"1999-01-01T01:01:01Z",
          "duck definition",
          "duck example",
          Seq("duck synonym"),
          Seq("duck antonym"),
          "duck translation",
          1d
        ),
        Row(
          "diehard",
          "noun",
          "ˈdʌɪhɑːd",
          Seq("diehard"),
          Seq("diehard Book"),
          1,
          t"2000-01-01T01:01:01Z",
          t"2000-01-01T01:01:01Z",
          "a person who strongly opposes change or who continues to support something in spite of opposition.",
          "a diehard Yankees fan",
          Seq("hard-line", "...", "blimp"),
          Seq("modernizer"),
          "diehard translation",
          1d
        )
      )
      area.updateArea(initialState)(initialState)

      val updatedDefinitions = createDataFrame(
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
          "duck example",
          Seq("duck synonym"),
          Seq("duck antonym")
        )
      )
      val newDefinitions = createDataFrame(
        fromSilverSchemaDdl,
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
          "a thin peevish voice",
          Seq("irritable", "...", "miffy"),
          Seq("affable", "easy-going")
        )
      )
      val enrichedNewDefinitions =
        newDefinitions.withColumn(TRANSLATION, lit("peevish translation")).withColumn(USAGE, lit(1d))

      val actual  = area.updateArea(updatedDefinitions)(enrichedNewDefinitions)
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
            "duck example",
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
            "a thin peevish voice",
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
            "a diehard Yankees fan",
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
      val firstTimestamp    = t"2021-01-01T01:01:01Z"
      val secondTimestamp   = t"2021-01-01T01:01:02Z"
      val timestampProvider = changingTimestampProvider(firstTimestamp, secondTimestamp)
      val area              = new GoldenArea(areaPath, null, null, timestampProvider)
      val initialState = createDataFrame(
        preMergeSchemaDdl,
        Row(
          "duck",
          "noun",
          "/dʌk/",
          Seq("duck"),
          Seq("duck Book"),
          1,
          t"1999-01-01T01:01:01Z",
          t"1999-01-01T01:01:01Z",
          "duck definition",
          "duck example",
          Seq("duck synonym"),
          Seq("duck antonym"),
          "duck translation",
          1d
        ),
        Row(
          "diehard",
          "noun",
          "ˈdʌɪhɑːd",
          Seq("diehard"),
          Seq("diehard Book"),
          1,
          t"2000-01-01T01:01:01Z",
          t"2000-01-01T01:01:01Z",
          "a person who strongly opposes change or who continues to support something in spite of opposition.",
          "a diehard Yankees fan",
          Seq("hard-line", "...", "blimp"),
          Seq("modernizer"),
          "diehard translation",
          1d
        )
      )
      area.updateArea(initialState)(initialState)

      val updates                = createDataFrame(fromSilverSchemaDdl)
      val enrichedNewDefinitions = createDataFrame(preMergeSchemaDdl)
      val actual                 = area.updateArea(updates)(enrichedNewDefinitions)
      val expected               = initialState.withColumn(UPDATED_AT, lit(firstTimestamp)).as[DictionaryRecord]
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
            null,
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
            "duck example",
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
            "duck example",
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
            "duck example definition",
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
            "spectators ducked for cover",
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
            null,
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
          "duck example",
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
          "duck example",
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
          null,
          Seq.empty,
          Seq.empty,
        ),
      )
      val sortedActual = actual.withColumn(BOOKS, array_sort(col(BOOKS))).withColumn(FORMS, array_sort(col(FORMS)))
      assertDataFrameDataEquals(expected, sortedActual)
    }
  }

}
