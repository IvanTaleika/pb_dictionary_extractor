package pb.dictionary.extraction.stage

import org.apache.spark.sql.{Dataset, SaveMode}
import pb.dictionary.extraction.ApplicationManagedAreaTestBase
import pb.dictionary.extraction.device.DeviceHighlight

class StageAreaTest extends ApplicationManagedAreaTestBase {

  override val areaName: String = "stage"

  describe("update method") {
    describe("Should parse highlights info, filtering out bookmarks") {
      describe("When table is empty") {
        it("Write everything") {
          import spark.implicits._
          val area = new StageArea(areaPath, testTimestampProvider)
          val highlights = spark.createDataset(
            Seq(
              DeviceHighlight(
                5228L,
                "{\"text\":\"Bookmark\"}",
                "A farewell to arms",
                "Ernest Miller Hemingway",
                1L
              ),
              DeviceHighlight(
                7832L,
                "{\"begin\":\"pbr:/word?page=78&offs=91\",\"end\":\"pbr:/word?page=78&over=97\",\"text\":\"yacht.\\\"\"}",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                2L
              ),
              DeviceHighlight(
                8920L,
                """{
                  "begin" : "pbr:/word?page=133&offs=1034",
                  "end" : "pbr:/word?page=133&over=1040",
                  "text" : "coroner",
                  "updated" : "2019-10-07T16:16:01Z"
                }""",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                3L
              ),
              DeviceHighlight(
                19475,
                "{\"begin\":\"pbr:/word?page=83&offs=366\",\"end\":\"pbr:/word?page=83&over=372\",\"text\":\"prickle\"}",
                "Harry Potter and the Sorcerer's Stone",
                "Joanne Kathleen Rowling",
                4L
              )
            )
          )
          val actual = area.upsert(highlights)
          val expected = spark.createDataset(
            Seq(
              HighlightedSentence(
                7832L,
                "yacht.\"",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                2L,
                testTimestamp
              ),
              HighlightedSentence(
                8920L,
                "coroner",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                3L,
                testTimestamp
              ),
              HighlightedSentence(
                19475L,
                "prickle",
                "Harry Potter and the Sorcerer's Stone",
                "Joanne Kathleen Rowling",
                4L,
                testTimestamp
              )
            ))

          assertDataFrameDataEquals(expected.toDF(), actual.toDF())
        }
      }
      describe("When table is not empty") {
        it("Write records with IDs greater than the highest recorded in the table and return written records") {
          import spark.implicits._
          val firstTimestamp            = t"2021-01-01T01:01:01Z"
          val secondTimestamp           = t"2021-01-01T01:01:02Z"
          val timestampProvider = changingTimestampProvider(firstTimestamp, secondTimestamp)
          val area = new StageArea(areaPath, timestampProvider)
          val currentState = spark.createDataset(
            Seq(
              DeviceHighlight(
                8920L,
                """{
                  "begin" : "pbr:/word?page=133&offs=1034",
                  "end" : "pbr:/word?page=133&over=1040",
                  "text" : "coroner",
                  "updated" : "2019-10-07T16:16:01Z"
                }""",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                3L
              )
            ))
          area.upsert(currentState)
          val highlights = spark.createDataset(
            Seq(
              DeviceHighlight(
                5228L,
                "{\"text\":\"Bookmark\"}",
                "A farewell to arms",
                "Ernest Miller Hemingway",
                1L
              ),
              DeviceHighlight(
                7832L,
                "{\"begin\":\"pbr:/word?page=78&offs=91\",\"end\":\"pbr:/word?page=78&over=97\",\"text\":\"yacht.\\\"\"}",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                2L
              ),
              DeviceHighlight(
                8920L,
                """{
                  "begin" : "pbr:/word?page=133&offs=1034",
                  "end" : "pbr:/word?page=133&over=1040",
                  "text" : "coroner",
                  "updated" : "2019-10-07T16:16:01Z"
                }""",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                3L
              ),
              DeviceHighlight(
                19475L,
                "{\"begin\":\"pbr:/word?page=83&offs=366\",\"end\":\"pbr:/word?page=83&over=372\",\"text\":\"prickle\"}",
                "Harry Potter and the Sorcerer's Stone",
                "Joanne Kathleen Rowling",
                4L
              )
            )
          )
          val actual = area.upsert(highlights)
          val expected = spark.createDataset(
            Seq(
              HighlightedSentence(
                19475L,
                "prickle",
                "Harry Potter and the Sorcerer's Stone",
                "Joanne Kathleen Rowling",
                4L,
                secondTimestamp
              ),
              HighlightedSentence(
                8920L,
                "coroner",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                3L,
                firstTimestamp
              )
            ))

          assertDataFrameDataEquals(expected.toDF(), actual.toDF())
        }
      }

      it("Should handle multiline records") {
        import spark.implicits._
        val firstTimestamp            = t"2021-01-01T01:01:01Z"
        val secondTimestamp           = t"2021-01-01T01:01:02Z"
        val timestampProvider = changingTimestampProvider(firstTimestamp, secondTimestamp)
        val area = new StageArea(areaPath, timestampProvider)
        val firstUpdate = spark.createDataset(
          Seq(
            DeviceHighlight(
              1L,
              "{\"begin\":\"pbr:/word?page=83&offs=366\",\"end\":\"pbr:/word?page=83&over=372\",\"text\":\"prickle\\nprickle\"}",
              "Harry Potter \nand the Sorcerer's Stone",
              "Joanne Kathleen \nRowling",
              1L
            ),
          ))
        area.upsert(firstUpdate)
        val secondUpdate = spark.createDataset(
          Seq(
            DeviceHighlight(
              1L,
              "{\"begin\":\"pbr:/word?page=83&offs=366\",\"end\":\"pbr:/word?page=83&over=372\",\"text\":\"prickle\\nprickle\"}",
              "Harry Potter \nand the Sorcerer's Stone",
              "Joanne Kathleen \nRowling",
              1L
            ),
            DeviceHighlight(
              2L,
              "{\"begin\":\"pbr:/word?page=78&offs=91\",\"end\":\"pbr:/word?page=78&over=97\",\"text\":\"yacht\\nyacht\"}",
              "The \nGreat Gatsby",
              "Francis \nScott Fitzgerald",
              2L
            )
          ))
        val actual = area.upsert(secondUpdate)
        val expected = spark.createDataset(
          Seq(
            HighlightedSentence(
              1L,
              "prickle\nprickle",
              "Harry Potter \nand the Sorcerer's Stone",
              "Joanne Kathleen \nRowling",
              1L,
              firstTimestamp
            ),
            HighlightedSentence(
              2L,
              "yacht\nyacht",
              "The \nGreat Gatsby",
              "Francis \nScott Fitzgerald",
              2L,
              secondTimestamp
            )
          ))
        assertDataFrameDataEquals(expected.toDF(), actual.toDF())
      }
    }

  }
}
