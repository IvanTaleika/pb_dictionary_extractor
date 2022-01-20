package pb.dictionary.extraction.bronze

import org.apache.spark.sql.functions._
import pb.dictionary.extraction.ApplicationManagedAreaTestBase
import pb.dictionary.extraction.stage.HighlightedText

import java.sql.Timestamp

class BronzeAreaTest extends ApplicationManagedAreaTestBase {

  override val areaName: String = "bronze"

  describe("update method") {
    describe("Should split text by punctuation, lowercase text, clean 1 and 0 length words, merge duplicated words") {
      describe("When cleansed text is not yet exists in the area") {
        it("Inserts the records") {
          import spark.implicits._
          val firstTimestamp    = t"2020-01-01T01:01:01Z"
          val secondTimestamp   = t"2021-01-01T01:01:01Z"
          val timestampProvider = changingTimestampProvider(firstTimestamp, secondTimestamp)
          val area              = new BronzeArea(areaPath, timestampProvider)

          val snapshot1 = spark.createDataset(
            Seq(
              HighlightedText(
                0L,
                "zero!",
                "Book0",
                "Author0",
                0L,
                t"2020-01-01T01:01:00Z"
              )
            )
          )

          val actual1 = area.upsert(snapshot1)
          val expected1 = spark.createDataset(
            Seq(
              CleansedText(
                "zero",
                Seq("`Book0` BY `Author0`"),
                1,
                t"1970-01-01T00:00:00Z",
                t"1970-01-01T00:00:00Z",
                firstTimestamp,
              )
            )
          )

          assertDataFrameDataEquals(expected1.toDF(), actual1.toDF())

          val snapshot2 = snapshot1.unionByName(
            spark.createDataset(
              Seq(
                HighlightedText(
                  1L,
                  "one ,two,three",
                  "Book1",
                  "Author1",
                  1L,
                  t"2021-01-01T01:01:00Z"
                ),
                HighlightedText(
                  2L,
                  ", one!TWO!three's",
                  "Book2",
                  "Author2",
                  2L,
                  t"2021-01-01T01:01:00Z"
                ),
                HighlightedText(
                  3L,
                  "Four.four",
                  "Book1",
                  "Author1",
                  3L,
                  t"2021-01-01T01:01:00Z"
                ),
                HighlightedText(
                  4L,
                  "A.a",
                  "Book1",
                  "Author1",
                  4L,
                  t"2021-01-01T01:01:00Z"
                ),
              )
            ))

          val actual2 = area.upsert(snapshot2)
          val expected2 = expected1.unionByName(
            spark.createDataset(
              Seq(
                CleansedText(
                  "one",
                  Seq("`Book1` BY `Author1`", "`Book2` BY `Author2`"),
                  2,
                  t"1970-01-01T00:00:01Z",
                  t"1970-01-01T00:00:02Z",
                  secondTimestamp,
                ),
                CleansedText(
                  "two",
                  Seq("`Book1` BY `Author1`", "`Book2` BY `Author2`"),
                  2,
                  t"1970-01-01T00:00:01Z",
                  t"1970-01-01T00:00:02Z",
                  secondTimestamp,
                ),
                CleansedText(
                  "three",
                  Seq("`Book1` BY `Author1`", "`Book2` BY `Author2`"),
                  2,
                  t"1970-01-01T00:00:01Z",
                  t"1970-01-01T00:00:02Z",
                  secondTimestamp,
                ),
                CleansedText(
                  "four",
                  Seq("`Book1` BY `Author1`"),
                  2,
                  t"1970-01-01T00:00:03Z",
                  t"1970-01-01T00:00:03Z",
                  secondTimestamp,
                ),
              )
            ))

          assertDataFrameDataEquals(expected2.toDF(), actual2.toDF())
        }
      }
      describe("When cleansed text already exists in the area") {
        it(
          "Update the records, summing the occurrences, aggregating books, selecting min and max occurrences timestamps") {
          import spark.implicits._
          val firstTimestamp    = t"2020-01-01T01:01:01Z"
          val secondTimestamp   = t"2021-01-01T01:01:01Z"
          val timestampProvider = changingTimestampProvider(firstTimestamp, secondTimestamp)
          val area              = new BronzeArea(areaPath, timestampProvider)

          val initialStageState = spark.createDataset(
            Seq(
              HighlightedText(
                1L,
                "one!Two",
                "Book1",
                "Author1",
                1L,
                t"2020-01-01T01:01:00Z"
              ),
              HighlightedText(
                2L,
                ", one",
                "Book2",
                "Author2",
                2L,
                t"2020-01-01T01:01:00Z"
              ),
              HighlightedText(
                3L,
                "Four.four",
                "Book1",
                "Author1",
                3L,
                t"2020-01-01T01:01:00Z"
              )
            )
          )
          area.upsert(initialStageState)

          val updates = spark.createDataset(
            Seq(
              HighlightedText(
                0L,
                "Two",
                "Book1",
                "Author1",
                0L,
                t"2021-01-01T01:01:01Z"
              ),
              HighlightedText(
                4L,
                " Four ",
                "Book3",
                "Author3",
                4L,
                t"2021-01-01T01:01:01Z"
              )
            )
          )

          val stageSnapshot = initialStageState.unionByName(updates)

          val actual = area.upsert(stageSnapshot)
          val expected =
            spark.createDataset(
              Seq(
                CleansedText(
                  "two",
                  Seq("`Book1` BY `Author1`"),
                  2,
                  t"1970-01-01T00:00:00Z",
                  t"1970-01-01T00:00:01Z",
                  secondTimestamp,
                ),
                CleansedText(
                  "four",
                  Seq("`Book1` BY `Author1`", "`Book3` BY `Author3`"),
                  3,
                  t"1970-01-01T00:00:03Z",
                  t"1970-01-01T00:00:04Z",
                  secondTimestamp,
                ),
                CleansedText(
                  "one",
                  Seq("`Book1` BY `Author1`", "`Book2` BY `Author2`"),
                  2,
                  t"1970-01-01T00:00:01Z",
                  t"1970-01-01T00:00:02Z",
                  firstTimestamp,
                )
              )
            )
          val sortedActual = actual.withColumn(CleansedText.BOOKS, array_sort(col(CleansedText.BOOKS)))

          assertDataFrameDataEquals(expected.toDF(), sortedActual)
        }
      }
    }

  }
}
