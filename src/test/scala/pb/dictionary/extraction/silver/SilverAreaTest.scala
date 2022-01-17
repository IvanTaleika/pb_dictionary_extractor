package pb.dictionary.extraction.silver

import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.functions.{col, lit}
import org.scalamock.function.FunctionAdapter1
import pb.dictionary.extraction.ApplicationManagedAreaTestBase
import pb.dictionary.extraction.bronze.CleansedWord

class SilverAreaTest extends ApplicationManagedAreaTestBase {

  override val areaName: String = "silver"

  describe("update method") {
    it("Should update text attributes without calling to definition API if the text is already defined") {
      import spark.implicits._

      val wordDefinitionApi = mock[WordDefinitionApi]
      (wordDefinitionApi.define _)
        .expects(new FunctionAdapter1[Dataset[CleansedWord], Boolean](ds => ds.isEmpty))
        .returns(spark.emptyDataset)
        .once()
      val area = new SilverArea(areaPath, wordDefinitionApi, testTimestampProvider)

      val initialState = spark.createDataset(
        Seq(
          DefinedWord(
            "die hard",
            Seq("testBook"),
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
          DefinedWord(
            "die hard",
            Seq("testBook"),
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
          DefinedWord(
            "diehard",
            Seq("testBook"),
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
          DefinedWord(
            "peevish",
            Seq("testBook"),
            1,
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
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

      initialState.write.format("delta").mode(SaveMode.Append).saveAsTable(area.fullTableName)

      val updates = spark.createDataset(
        Seq(
          CleansedWord(
            "die hard",
            Seq("testBook"),
            2,
            t"1999-01-02T01:01:01Z",
            t"1999-01-03T01:01:01Z",
            t"1999-01-04T01:01:01Z"
          ),
          CleansedWord(
            "peevish",
            Seq("testBook", "testBook2"),
            3,
            t"1998-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            t"1999-01-04T01:01:01Z"
          )
        )
      )
      val result = area.upsert(updates, null)
      val expectedResult = spark.createDataset(
        Seq(
          DefinedWord(
            "die hard",
            Seq("testBook"),
            2,
            t"1999-01-02T01:01:01Z",
            t"1999-01-03T01:01:01Z",
            testTimestamp,
            "die hard",
            null,
            null,
            "disappear or change very slowly.",
            "old habits die hard",
            Seq.empty,
            Seq.empty
          ),
          DefinedWord(
            "die hard",
            Seq("testBook"),
            2,
            t"1999-01-02T01:01:01Z",
            t"1999-01-03T01:01:01Z",
            testTimestamp,
            "diehard",
            "ˈdʌɪhɑːd",
            "noun",
            "a person who strongly opposes change or who continues to support something in spite of opposition.",
            "a diehard Yankees fan",
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer")
          ),
          DefinedWord(
            "peevish",
            Seq("testBook", "testBook2"),
            3,
            t"1998-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            testTimestamp,
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
      assertDataFrameNoOrderEquals(result.toDF(), expectedResult.toDF())

      val actualState = area.snapshot
      val expectedState = spark.createDataset(
        Seq(
          DefinedWord(
            "die hard",
            Seq("testBook"),
            2,
            t"1999-01-02T01:01:01Z",
            t"1999-01-03T01:01:01Z",
            testTimestamp,
            "die hard",
            null,
            null,
            "disappear or change very slowly.",
            "old habits die hard",
            Seq.empty,
            Seq.empty
          ),
          DefinedWord(
            "die hard",
            Seq("testBook"),
            2,
            t"1999-01-02T01:01:01Z",
            t"1999-01-03T01:01:01Z",
            testTimestamp,
            "diehard",
            "ˈdʌɪhɑːd",
            "noun",
            "a person who strongly opposes change or who continues to support something in spite of opposition.",
            "a diehard Yankees fan",
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer")
          ),
          DefinedWord(
            "diehard",
            Seq("testBook"),
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
          DefinedWord(
            "peevish",
            Seq("testBook", "testBook2"),
            3,
            t"1998-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            testTimestamp,
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
      assertDataFrameNoOrderEquals(actualState.toDF(), expectedState.toDF())
    }

    it("Should send text to definition API and insert its result into the table if text isn't defined yet") {
      import spark.implicits._

      val updates = spark.createDataset(
        Seq(
          CleansedWord(
            "agsbgf",
            Seq("testBook"),
            2,
            t"1999-01-02T01:01:01Z",
            t"1999-01-03T01:01:01Z",
            t"1999-01-04T01:01:01Z"
          ),
          CleansedWord(
            "diehard",
            Seq("testBook", "testBook2"),
            3,
            t"1998-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            t"1999-01-04T01:01:01Z"
          )
        )
      )

      val definedUpdates = spark.createDataset(
        Seq(
          DefinedWord(
            "agsbgf",
            Seq("testBook"),
            2,
            t"1999-01-02T01:01:01Z",
            t"1999-01-03T01:01:01Z",
            t"1999-01-04T01:01:01Z",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
          ),
          DefinedWord(
            "diehard",
            Seq("testBook", "testBook2"),
            3,
            t"1998-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            t"1999-01-04T01:01:01Z",
            "diehard",
            "ˈdʌɪhɑːd",
            "noun",
            "a person who strongly opposes change or who continues to support something in spite of opposition.",
            "a diehard Yankees fan",
            Seq("hard-line", "...", "blimp"),
            Seq("modernizer")
          )
        )
      )

      val wordDefinitionApi = mock[WordDefinitionApi]
      (wordDefinitionApi.define _)
        .expects(new FunctionAdapter1[Dataset[CleansedWord], Boolean](ds => {
          assertDataFrameNoOrderEquals(ds.toDF(), updates.toDF())
          true
        }))
        .returns(definedUpdates)
        .once()
      val area = new SilverArea(areaPath, wordDefinitionApi, testTimestampProvider)

      val initialState = spark.createDataset(
        Seq(
          DefinedWord(
            "die hard",
            Seq("testBook"),
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
          DefinedWord(
            "die hard",
            Seq("testBook"),
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
          DefinedWord(
            "peevish",
            Seq("testBook"),
            1,
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
            t"1999-01-01T01:01:01Z",
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

      initialState.write.format("delta").mode(SaveMode.Append).saveAsTable(area.fullTableName)

      val result = area.upsert(updates, null)
      val expectedResult = definedUpdates.withColumn(DefinedWord.UPDATED_AT, lit(testTimestamp))
      assertDataFrameDataEquals(result.toDF(), expectedResult)

      val actualState = area.snapshot
      val expectedState = initialState.toDF().unionByName(expectedResult)
      assertDataFrameDataEquals(actualState.toDF(), expectedState)
    }
  }
}
