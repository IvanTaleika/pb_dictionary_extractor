package pb.dictionary.extraction.silver

import org.apache.http.{HttpHost, HttpRequest, HttpStatus, HttpVersion}
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.message.BasicStatusLine
import org.apache.http.protocol.HttpContext
import org.apache.spark.SparkException
import pb.dictionary.extraction.{RemoteHttpEnrichmentException, TestBase}
import pb.dictionary.extraction.bronze.CleansedText
import pb.dictionary.extraction.silver.DictionaryApiDevWordDefiner.DictionaryApiDevDfEnricher

class DictionaryApiDevWordDefinerTest extends TestBase {
  import pb.dictionary.extraction.bronze.CleansedText._

  val sourceSchema =
    s"$TEXT String, $BOOKS Array<String>, $OCCURRENCES Int, $FIRST_OCCURRENCE Timestamp, $LATEST_OCCURRENCE Timestamp, $UPDATED_AT Timestamp"

  def createUndefinedDf(text: String) = {
    import spark.implicits._
    spark.createDataset(
      Seq(
        CleansedText(
          text,
          Seq("testBook"),
          1,
          t"2021-01-01T01:01:01Z",
          t"2021-01-01T01:01:01Z",
          t"2021-01-01T01:01:01Z",
        )
      ))
  }

  describe("define method") {

    describe("Should abort data enrichment, throwing an exception") {

      it("when API response with unknown error code") {
        val df = createUndefinedDf("hello")
        val testClient = new TestDictionaryApiDevEnricher(
          HttpStatus.SC_SERVICE_UNAVAILABLE,
          ""
        )
        val definer = new DictionaryApiDevWordDefiner(new DictionaryApiDevDfEnricher(testClient))
        val actual = the[SparkException] thrownBy (definer.define(df).collect())
        actual.getCause shouldBe a [RemoteHttpEnrichmentException]
      }
    }

    describe("Should populate definition columns with nulls") {
      it("When no definition was found for a text") {
        import spark.implicits._
        val df = createUndefinedDf("agsbgf")
        val testClient = new TestDictionaryApiDevEnricher(
          HttpStatus.SC_NOT_FOUND,
          """
            |{
            |  "title": "No Definitions Found",
            |  "message": "Sorry pal, we couldn't find definitions for the word you were looking for.",
            |  "resolution": "You can try the search again at later time or head to the web instead."
            |}
            |""".stripMargin
        )
        val definer = new DictionaryApiDevWordDefiner(new DictionaryApiDevDfEnricher(testClient))
        val actual  = definer.define(df)
        val expected = spark.createDataset(
          Seq(
            DefinedText(
              "agsbgf",
              Seq("testBook"),
              1,
              t"2021-01-01T01:01:01Z",
              t"2021-01-01T01:01:01Z",
              t"2021-01-01T01:01:01Z",
              null,
              null,
              null,
              null,
              null,
              null,
              null
            )),
        )
        assertDataFrameDataInColumnsEqual(expected.toDF, actual.toDF)
      }
    }

    describe("Should populate definition columns with returned values") {
      it("when text has a single meaning only") {
        import spark.implicits._
        val df = createUndefinedDf("peevish")

        val testClient = new TestDictionaryApiDevEnricher(
          HttpStatus.SC_OK,
          """
            |[
            |  {
            |    "word": "peevish",
            |    "phonetic": "ˈpiːvɪʃ",
            |    "phonetics": [
            |      {
            |        "text": "ˈpiːvɪʃ",
            |        "audio": "//ssl.gstatic.com/dictionary/static/sounds/20200429/peevish--_gb_1.mp3"
            |      }
            |    ],
            |    "origin": "late Middle English (in the sense ‘perverse, coy’): of unknown origin.",
            |    "meanings": [
            |      {
            |        "partOfSpeech": "adjective",
            |        "definitions": [
            |          {
            |            "definition": "having or showing an irritable disposition.",
            |            "example": "a thin peevish voice",
            |            "synonyms": [
            |              "irritable",
            |              "...",
            |              "miffy"
            |            ],
            |            "antonyms": [
            |              "affable",
            |              "easy-going"
            |            ]
            |          }
            |        ]
            |      }
            |    ]
            |  }
            |]
            |""".stripMargin
        )
        val definer = new DictionaryApiDevWordDefiner(new DictionaryApiDevDfEnricher(testClient))
        val actual  = definer.define(df)
        val expected = spark.createDataset(
          Seq(
            DefinedText(
              "peevish",
              Seq("testBook"),
              1,
              t"2021-01-01T01:01:01Z",
              t"2021-01-01T01:01:01Z",
              t"2021-01-01T01:01:01Z",
              "peevish",
              "ˈpiːvɪʃ",
              "adjective",
              "having or showing an irritable disposition.",
              Seq("a thin peevish voice"),
              Seq("irritable", "...", "miffy"),
              Seq("affable", "easy-going")
            ))
        )
        assertDataFrameDataInColumnsEqual(expected.toDF, actual.toDF)
      }

      describe("And create a separate row in the output for each definition") {
        it("when text has multiple origins") {
          import spark.implicits._
          val df = createUndefinedDf("die hard")
          val testClient = new TestDictionaryApiDevEnricher(
            HttpStatus.SC_OK,
            """
              |[
              |  {
              |    "word": "die hard",
              |    "phonetics": [
              |      {}
              |    ],
              |    "meanings": [
              |      {
              |        "definitions": [
              |          {
              |            "definition": "disappear or change very slowly.",
              |            "example": "old habits die hard",
              |            "synonyms": [],
              |            "antonyms": []
              |          }
              |        ]
              |      }
              |    ]
              |  },
              |  {
              |    "word": "diehard",
              |    "phonetic": "ˈdʌɪhɑːd",
              |    "phonetics": [
              |      {
              |        "text": "ˈdʌɪhɑːd",
              |        "audio": "//ssl.gstatic.com/dictionary/static/sounds/20200429/diehard--_gb_1.mp3"
              |      }
              |    ],
              |    "origin": "mid 19th century: from die hard (see die1).",
              |    "meanings": [
              |      {
              |        "partOfSpeech": "noun",
              |        "definitions": [
              |          {
              |            "definition": "a person who strongly opposes change or who continues to support something in spite of opposition.",
              |            "example": "a diehard Yankees fan",
              |            "synonyms": [
              |              "hard-line",
              |              "...",
              |              "blimp"
              |            ],
              |            "antonyms": [
              |              "modernizer"
              |            ]
              |          }
              |        ]
              |      }
              |    ]
              |  }
              |]
              |""".stripMargin
          )
          val definer = new DictionaryApiDevWordDefiner(new DictionaryApiDevDfEnricher(testClient))
          val actual  = definer.define(df)
          val expected = spark.createDataset(
            Seq(
              DefinedText(
                "die hard",
                Seq("testBook"),
                1,
                t"2021-01-01T01:01:01Z",
                t"2021-01-01T01:01:01Z",
                t"2021-01-01T01:01:01Z",
                "die hard",
                null,
                null,
                "disappear or change very slowly.",
                Seq("old habits die hard"),
                Seq.empty,
                Seq.empty
              ),
              DefinedText(
                "die hard",
                Seq("testBook"),
                1,
                t"2021-01-01T01:01:01Z",
                t"2021-01-01T01:01:01Z",
                t"2021-01-01T01:01:01Z",
                "diehard",
                "ˈdʌɪhɑːd",
                "noun",
                "a person who strongly opposes change or who continues to support something in spite of opposition.",
                Seq("a diehard Yankees fan"),
                Seq("hard-line", "...", "blimp"),
                Seq("modernizer")
              )
            ))
          assertDataFrameDataInColumnsEqual(expected.toDF, actual.toDF)
        }

        it("when text has multiple meanings") {
          import spark.implicits._
          val df = createUndefinedDf("duck")
          val testClient = new TestDictionaryApiDevEnricher(
            HttpStatus.SC_OK,
            """
              |[
              |  {
              |    "word": "duck",
              |    "phonetic": "dʌk",
              |    "phonetics": [
              |      {
              |        "text": "dʌk",
              |        "audio": "//ssl.gstatic.com/dictionary/static/sounds/20200429/duck--_gb_1.mp3"
              |      }
              |    ],
              |    "origin": "Old English duce, from the Germanic base of duck2 (expressing the notion of ‘diving bird’).",
              |    "meanings": [
              |      {
              |        "partOfSpeech": "noun",
              |        "definitions": [
              |          {
              |            "definition": "a waterbird with a broad blunt bill, short legs, webbed feet, and a waddling gait.",
              |            "synonyms": [],
              |            "antonyms": []
              |          },
              |          {
              |            "definition": "a pure white thin-shelled bivalve mollusc found off the Atlantic coasts of America.",
              |            "synonyms": [],
              |            "antonyms": []
              |          },
              |          {
              |            "definition": "an amphibious transport vehicle.",
              |            "example": "visitors can board an amphibious duck to explore the city",
              |            "synonyms": [],
              |            "antonyms": []
              |          }
              |        ]
              |      }
              |    ]
              |  }
              |]
              |""".stripMargin
          )
          val definer = new DictionaryApiDevWordDefiner(new DictionaryApiDevDfEnricher(testClient))
          val actual  = definer.define(df)
          val expected = spark.createDataset(
            Seq(
              DefinedText(
                "duck",
                Seq("testBook"),
                1,
                t"2021-01-01T01:01:01Z",
                t"2021-01-01T01:01:01Z",
                t"2021-01-01T01:01:01Z",
                "duck",
                "dʌk",
                "noun",
                "a waterbird with a broad blunt bill, short legs, webbed feet, and a waddling gait.",
                Seq.empty,
                Seq.empty,
                Seq.empty
              ),
              DefinedText(
                "duck",
                Seq("testBook"),
                1,
                t"2021-01-01T01:01:01Z",
                t"2021-01-01T01:01:01Z",
                t"2021-01-01T01:01:01Z",
                "duck",
                "dʌk",
                "noun",
                "a pure white thin-shelled bivalve mollusc found off the Atlantic coasts of America.",
                Seq.empty,
                Seq.empty,
                Seq.empty
              ),
              DefinedText(
                "duck",
                Seq("testBook"),
                1,
                t"2021-01-01T01:01:01Z",
                t"2021-01-01T01:01:01Z",
                t"2021-01-01T01:01:01Z",
                "duck",
                "dʌk",
                "noun",
                "an amphibious transport vehicle.",
                Seq("visitors can board an amphibious duck to explore the city"),
                Seq.empty,
                Seq.empty
              ),
            ))
          assertDataFrameDataInColumnsEqual(expected.toDF, actual.toDF)
        }

      }
    }
  }

}

abstract class MockableCloseableHttpClient extends CloseableHttpClient {
  override def doExecute(target: HttpHost, request: HttpRequest, context: HttpContext): CloseableHttpResponse
}

// We must create a subclass to keep object serializable
class TestDictionaryApiDevEnricher(responseCode: Int, responseBody: String) extends DictionaryApiDevEnricher(None) {
  import org.mockito.ArgumentMatchers._
  import org.mockito.Mockito._
  override def httpClient: CloseableHttpClient = {
    val httpClientMock = mock(classOf[MockableCloseableHttpClient])

    val httpResponseMock = mock(classOf[CloseableHttpResponse])
    val statusLine       = new BasicStatusLine(HttpVersion.HTTP_1_1, responseCode, "")
    when(httpResponseMock.getStatusLine).thenReturn(statusLine)
    val responseEntity = new StringEntity(responseBody, ContentType.APPLICATION_JSON)
    when(httpResponseMock.getEntity).thenReturn(responseEntity)

    when(httpClientMock.execute(any())).thenReturn(httpResponseMock)

    httpClientMock
  }
}
