package pb.dictionary.extraction.bronze

import org.apache.spark.sql.functions._
import pb.dictionary.extraction.ApplicationManagedAreaTestBase
import pb.dictionary.extraction.stage.HighlightedSentence

import java.sql.Timestamp

class BronzeAreaTest extends ApplicationManagedAreaTestBase {

  override val areaName: String = "bronze"

  describe("update method") {
    describe("Should split text by punctuation, lowercase text, clean 1 and 0 length words, merge duplicated words") {
      describe("When cleansed text is not yet exists in the area") {
        it("Inserts the records") {
          import spark.implicits._
          val firstTimestamp    = t"2021-01-01T01:01:01Z"
          val secondTimestamp   = t"2021-01-01T01:01:02Z"
          val timestampProvider = changingTimestampProvider(firstTimestamp, secondTimestamp)
          val area              = new BronzeArea(areaPath, timestampProvider)

          val updates1 = spark.createDataset(
            Seq(
              HighlightedSentence(
                0L,
                "zero!",
                "Book0",
                "Author0",
                0L
              )
            )
          )

          val actual1 = area.upsert(updates1, null)
          val expected1 = spark.createDataset(
            Seq(
              CleansedWord(
                "zero",
                Seq("`Book0` BY `Author0`"),
                1,
                t"1970-01-01T00:00:00Z",
                t"1970-01-01T00:00:00Z",
                firstTimestamp,
              )
            )
          )

          assertDataFrameDataEquals(actual1.toDF(), expected1.toDF())
          assertDataFrameDataEquals(area.snapshot.toDF(), expected1.toDF())

          val updates2 = spark.createDataset(
            Seq(
              HighlightedSentence(
                1L,
                "one ,two,three",
                "Book1",
                "Author1",
                1L
              ),
              HighlightedSentence(
                2L,
                ", one!TWO!three's",
                "Book2",
                "Author2",
                2L
              ),
              HighlightedSentence(
                3L,
                "Four.four",
                "Book1",
                "Author1",
                3L
              ),
              HighlightedSentence(
                4L,
                "A.a",
                "Book1",
                "Author1",
                4L
              ),
            )
          )

          val actual2 = area.upsert(updates2, null)
          val expected2 = spark.createDataset(
            Seq(
              CleansedWord(
                "one",
                Seq("`Book1` BY `Author1`", "`Book2` BY `Author2`"),
                2,
                t"1970-01-01T00:00:01Z",
                t"1970-01-01T00:00:02Z",
                secondTimestamp,
              ),
              CleansedWord(
                "two",
                Seq("`Book1` BY `Author1`", "`Book2` BY `Author2`"),
                2,
                t"1970-01-01T00:00:01Z",
                t"1970-01-01T00:00:02Z",
                secondTimestamp,
              ),
              CleansedWord(
                "three",
                Seq("`Book1` BY `Author1`", "`Book2` BY `Author2`"),
                2,
                t"1970-01-01T00:00:01Z",
                t"1970-01-01T00:00:02Z",
                secondTimestamp,
              ),
              CleansedWord(
                "four",
                Seq("`Book1` BY `Author1`"),
                2,
                t"1970-01-01T00:00:03Z",
                t"1970-01-01T00:00:03Z",
                secondTimestamp,
              ),
            )
          )

          val expectedState2 = expected1.unionByName(expected2)

          assertDataFrameDataEquals(actual2.toDF(), expected2.toDF())
          assertDataFrameDataEquals(area.snapshot.toDF(), expectedState2.toDF())
        }
      }
      describe("When cleansed text already exists in the area") {
        it(
          "Update the records, summing the occurrences, aggregating books, selecting min and max occurrences timestamps") {
          import spark.implicits._
          val firstTimestamp            = t"2021-01-01T01:01:01Z"
          val secondTimestamp           = t"2021-01-01T01:01:02Z"
          val timestampProvider = changingTimestampProvider(firstTimestamp, secondTimestamp)
          val area              = new BronzeArea(areaPath, timestampProvider)

          val initialState = spark.createDataset(
            Seq(
              HighlightedSentence(
                1L,
                "one!Two",
                "Book1",
                "Author1",
                1L
              ),
              HighlightedSentence(
                2L,
                ", one",
                "Book2",
                "Author2",
                2L
              ),
              HighlightedSentence(
                3L,
                "Four.four",
                "Book1",
                "Author1",
                3L
              )
            )
          )
          area.upsert(initialState, null)

          val updates = spark.createDataset(
            Seq(
              HighlightedSentence(
                0L,
                "Two",
                "Book1",
                "Author1",
                0L
              ),
              HighlightedSentence(
                4L,
                " Four ",
                "Book3",
                "Author3",
                4L
              )
            )
          )

          val actual = area.upsert(updates, null)
          val expected = spark.createDataset(
            Seq(
              CleansedWord(
                "two",
                Seq("`Book1` BY `Author1`"),
                2,
                t"1970-01-01T00:00:00Z",
                t"1970-01-01T00:00:01Z",
                secondTimestamp,
              ),
              CleansedWord(
                "four",
                Seq("`Book1` BY `Author1`", "`Book3` BY `Author3`"),
                3,
                t"1970-01-01T00:00:03Z",
                t"1970-01-01T00:00:04Z",
                secondTimestamp,
              ),
            )
          )

          val expectedState = expected.unionByName(
            spark.createDataset(
              Seq(
                CleansedWord(
                  "one",
                  Seq("`Book1` BY `Author1`", "`Book2` BY `Author2`"),
                  2,
                  t"1970-01-01T00:00:01Z",
                  t"1970-01-01T00:00:02Z",
                  firstTimestamp,
                )
              )
            )
          )
          val sortedActual = actual.withColumn(CleansedWord.BOOKS, array_sort(col(CleansedWord.BOOKS)))
          val sortedActualState =
            area.snapshot.withColumn(CleansedWord.BOOKS, array_sort(col(CleansedWord.BOOKS))).toDF()

          assertDataFrameDataEquals(sortedActual, expected.toDF())
          assertDataFrameDataEquals(sortedActualState, expectedState.toDF())
        }
      }
    }

  }
}
