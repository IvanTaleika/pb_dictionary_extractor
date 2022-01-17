package pb.dictionary.extraction.stage

import org.apache.spark.sql.{Dataset, SaveMode}
import pb.dictionary.extraction.ApplicationManagedAreaTestBase
import pb.dictionary.extraction.device.DeviceHighlight

class StageAreaTest extends ApplicationManagedAreaTestBase {

  override val areaName: String = "stage"

  describe("update method") {
    describe("Should parse highlights info, filtering out bookmarks") {
      describe("When table is empty") {
        it("Write everything and return written record") {
          import spark.implicits._
          val area = new StageArea(areaPath)
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
          val actual = area.upsert(highlights, null)
          val expected = spark.createDataset(
            Seq(
              HighlightedSentence(
                7832L,
                "yacht.\"",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                2L
              ),
              HighlightedSentence(
                8920L,
                "coroner",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                3L
              ),
              HighlightedSentence(
                19475L,
                "prickle",
                "Harry Potter and the Sorcerer's Stone",
                "Joanne Kathleen Rowling",
                4L
              )
            ))

          assertDataFrameDataEquals(actual.toDF(), expected.toDF())
          assertDataFrameDataEquals(area.snapshot.toDF(), expected.toDF())
        }
      }
      describe("When table is not empty") {
        it("Write records with IDs greater than the highest recorded in the table and return written records") {
          import spark.implicits._
          val area = new StageArea(areaPath)
          val currentState = spark.createDataset(
            Seq(
              HighlightedSentence(
                8920L,
                "coroner",
                "The Great Gatsby",
                "Francis Scott Fitzgerald",
                3L
              )
            ))
          currentState.write.mode(SaveMode.Append).format("csv").saveAsTable(area.fullTableName)
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
          val actual = area.upsert(highlights, null)
          val expected = spark.createDataset(
            Seq(
              HighlightedSentence(
                19475L,
                "prickle",
                "Harry Potter and the Sorcerer's Stone",
                "Joanne Kathleen Rowling",
                4L
              )
            ))
          val expectedState = currentState.unionByName(expected)

          assertDataFrameDataEquals(actual.toDF(), expected.toDF())
          assertDataFrameDataEquals(area.snapshot.toDF(), expectedState.toDF())
        }
      }

      it("Should handle multiline records") {
        import spark.implicits._
        val area = new StageArea(areaPath)
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
        area.upsert(firstUpdate, null)
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
        area.upsert(secondUpdate, null)
        val expectedState = spark.createDataset(
          Seq(
            HighlightedSentence(
              1L,
              "prickle\nprickle",
              "Harry Potter \nand the Sorcerer's Stone",
              "Joanne Kathleen \nRowling",
              1L
            ),
            HighlightedSentence(
              2L,
              "yacht\nyacht",
              "The \nGreat Gatsby",
              "Francis \nScott Fitzgerald",
              2L
            )
          ))
        assertDataFrameDataEquals(area.snapshot.toDF(), expectedState.toDF())
      }
    }

  }
}
