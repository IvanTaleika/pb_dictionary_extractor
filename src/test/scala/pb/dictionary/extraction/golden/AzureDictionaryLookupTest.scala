package pb.dictionary.extraction.golden

import org.apache.http.{HttpResponse, HttpStatus}
import org.apache.http.client.methods.{HttpPost, HttpUriRequest}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.SparkException
import org.mockito.{ArgumentMatchers, Mockito}
import pb.dictionary.extraction.{EnricherTestUtils, TestBase}
import pb.dictionary.extraction.golden.AzureDictionaryLookup.{DfEnricher, DictionaryLookupRequest}
import pb.dictionary.extraction.golden.AzureDictionaryLookupEnricher.{AZURE_SERVICE_ERROR_RETRY_INTERVAL, MAX_RETRIES}
import EnricherTestUtils._
import pb.dictionary.extraction.enrichment.RemoteHttpEnrichmentException

import java.nio.charset.StandardCharsets

class AzureDictionaryLookupTest extends TestBase {

  import pb.dictionary.extraction.golden.VocabularyRecord._

  val sourceSchema = s"$NORMALIZED_TEXT String, $PART_OF_SPEECH String, $FORMS array<string>, $DEFINITION String"
  val finalSchema = s"$sourceSchema, $TRANSLATIONS array<string>"

  describe("translate method") {
    it(s"Should make a single request for all the records with the same `$NORMALIZED_TEXT` attribute") {
      // format: off
      val df = createDataFrame(
        sourceSchema,
        Row("verbatim", "noun", Seq("verbatim"), "A word-for-word report of a speech."),
        Row("verbatim", "adverb", Seq("verbatim"), "Orally; verbally."),
        Row("duck", "verb", Seq("duck"), "To evade doing something."),
        Row("duck", "verb", Seq("duck"), "To quickly lower (the head) in order to prevent it from being struck by something."),
        Row("duck", "noun", Seq("duck"), "The flesh of a duck used as food."),
      )
      // format: on

      val testClient = new TestAzureDictionaryLookupEnricher(
        EnricherResponseInfo(
          "verbatim",
          """
            |[
            |  {
            |    "normalizedSource": "verbatim",
            |    "displaySource": "verbatim",
            |    "translations": []
            |  }
            |]""".stripMargin,
          HttpStatus.SC_OK
        ),
        EnricherResponseInfo(
          "duck",
          """
            |[
            |  {
            |    "normalizedSource": "duck",
            |    "displaySource": "duck",
            |    "translations": []
            |  }
            |]""".stripMargin,
          HttpStatus.SC_OK
        )
      )

      val translationClient = new AzureDictionaryLookup(new DfEnricher(testClient))
      val actual = translationClient.translate(df)
      // format: off
      val expected = createDataFrame(
        finalSchema,
        Row("verbatim", "noun", Seq("verbatim"), "A word-for-word report of a speech.", Seq.empty),
        Row("verbatim", "adverb", Seq("verbatim"), "Orally; verbally.", Seq.empty),
        Row("duck", "verb", Seq("duck"), "To evade doing something.", Seq.empty),
        Row("duck", "verb", Seq("duck"), "To quickly lower (the head) in order to prevent it from being struck by something.", Seq.empty),
        Row("duck", "noun", Seq("duck"), "The flesh of a duck used as food.", Seq.empty),
      )
      // format: on

      assertDataFrameDataInColumnsEqual(actual, expected)

      Mockito.verify(testClient.httpClient, Mockito.atMost(2)).execute(ArgumentMatchers.any())

    }

    describe("Should populate translation column with a sequence of translations based on type of speech for a translation") {
      it("when only a single translation was found for a text") {
        // format: off
        val df = createDataFrame(
          sourceSchema,
          Row("verbatim", "adjective", Seq("verbatim"), "(of a person) Able to take down a speech word for word, especially in shorthand."),
          Row("duck", "verb", Seq("duck"), "To evade doing something."),
          Row("duck", "noun", Seq("duck"), "The flesh of a duck used as food."),
        )
        // format: on

        val testClient = new TestAzureDictionaryLookupEnricher(
          EnricherResponseInfo(
            "verbatim",
            """
              |[
              |  {
              |    "normalizedSource": "verbatim",
              |    "displaySource": "verbatim",
              |    "translations": [
              |      {
              |        "normalizedTarget": "дословно",
              |        "displayTarget": "дословно",
              |        "posTag": "ADJ",
              |        "confidence": 0.4134,
              |        "prefixWord": "",
              |        "backTranslations": [
              |          {
              |            "normalizedText": "literally",
              |            "displayText": "literally",
              |            "numExamples": 10,
              |            "frequencyCount": 77
              |          },
              |          {
              |            "normalizedText": "verbatim",
              |            "displayText": "verbatim",
              |            "numExamples": 3,
              |            "frequencyCount": 27
              |          }
              |        ]
              |      }
              |    ]
              |  }
              |]""".stripMargin, HttpStatus.SC_OK),
          EnricherResponseInfo(
            "duck",
            """
              |[
              |  {
              |    "normalizedSource": "duck",
              |    "displaySource": "duck",
              |    "translations": [
              |      {
              |        "normalizedTarget": "утка",
              |        "displayTarget": "утка",
              |        "posTag": "NOUN",
              |        "confidence": 0.3795,
              |        "prefixWord": "",
              |        "backTranslations": [
              |          {
              |            "normalizedText": "duck",
              |            "displayText": "duck",
              |            "numExamples": 15,
              |            "frequencyCount": 948
              |          },
              |          {
              |            "normalizedText": "weft",
              |            "displayText": "weft",
              |            "numExamples": 0,
              |            "frequencyCount": 12
              |          }
              |        ]
              |      }
              |    ]
              |  }
              |]""".stripMargin,
            HttpStatus.SC_OK)
        )

        val translationClient = new AzureDictionaryLookup(new DfEnricher(testClient))
        val actual = translationClient.translate(df)
        // format: off
        val expected = createDataFrame(
          finalSchema,
          Row("verbatim", "adjective", Seq("verbatim"), "(of a person) Able to take down a speech word for word, especially in shorthand.", Seq("дословно")),
          Row("duck", "verb", Seq("duck"), "To evade doing something.", Seq.empty),
          Row("duck", "noun", Seq("duck"), "The flesh of a duck used as food.", Seq("утка")),
        )
        // format: on

        assertDataFrameDataInColumnsEqual(actual, expected)
      }

      it("when multiple translations were found for a text") {
        // format: off
        val df = createDataFrame(
          sourceSchema,
          Row("duck", "verb", Seq("duck"), "To quickly lower the head or body in order to prevent it from being struck by something."),
          Row("duck", "verb", Seq("duck"), "To evade doing something."),
          Row("duck", "noun", Seq("duck"), "An aquatic bird of the family Anatidae, having a flat bill and webbed feet."),
          Row("duck", "noun", Seq("duck"), "The flesh of a duck used as food."),
        )
        // format: on

        val testClient = new TestAzureDictionaryLookupEnricher(
          EnricherResponseInfo(
            "duck",
            """
              |[
              |  {
              |    "normalizedSource": "duck",
              |    "displaySource": "duck",
              |    "translations": [
              |      {
              |        "normalizedTarget": "утка",
              |        "displayTarget": "утка",
              |        "posTag": "NOUN",
              |        "confidence": 0.3795,
              |        "prefixWord": "",
              |        "backTranslations": [
              |          {
              |            "normalizedText": "duck",
              |            "displayText": "duck",
              |            "numExamples": 15,
              |            "frequencyCount": 948
              |          },
              |          {
              |            "normalizedText": "weft",
              |            "displayText": "weft",
              |            "numExamples": 0,
              |            "frequencyCount": 12
              |          }
              |        ]
              |      },
              |      {
              |        "normalizedTarget": "дак",
              |        "displayTarget": "Дак",
              |        "posTag": "NOUN",
              |        "confidence": 0.2098,
              |        "prefixWord": "",
              |        "backTranslations": [
              |          {
              |            "normalizedText": "duck",
              |            "displayText": "duck",
              |            "numExamples": 15,
              |            "frequencyCount": 303
              |          },
              |          {
              |            "normalizedText": "dak",
              |            "displayText": "dak",
              |            "numExamples": 2,
              |            "frequencyCount": 35
              |          },
              |          {
              |            "normalizedText": "dac",
              |            "displayText": "DAC",
              |            "numExamples": 4,
              |            "frequencyCount": 8
              |          }
              |        ]
              |      },
              |      {
              |        "normalizedTarget": "утиная",
              |        "displayTarget": "Утиная",
              |        "posTag": "NOUN",
              |        "confidence": 0.2071,
              |        "prefixWord": "",
              |        "backTranslations": [
              |          {
              |            "normalizedText": "duck",
              |            "displayText": "duck",
              |            "numExamples": 8,
              |            "frequencyCount": 122
              |          }
              |        ]
              |      },
              |      {
              |        "normalizedTarget": "уток",
              |        "displayTarget": "уток",
              |        "posTag": "NOUN",
              |        "confidence": 0.0771,
              |        "prefixWord": "",
              |        "backTranslations": [
              |          {
              |            "normalizedText": "ducks",
              |            "displayText": "ducks",
              |            "numExamples": 5,
              |            "frequencyCount": 365
              |          },
              |          {
              |            "normalizedText": "duck",
              |            "displayText": "duck",
              |            "numExamples": 7,
              |            "frequencyCount": 46
              |          },
              |          {
              |            "normalizedText": "weft",
              |            "displayText": "weft",
              |            "numExamples": 0,
              |            "frequencyCount": 19
              |          }
              |        ]
              |      },
              |      {
              |        "normalizedTarget": "утенок",
              |        "displayTarget": "утенок",
              |        "posTag": "NOUN",
              |        "confidence": 0.0653,
              |        "prefixWord": "",
              |        "backTranslations": [
              |          {
              |            "normalizedText": "duckling",
              |            "displayText": "duckling",
              |            "numExamples": 5,
              |            "frequencyCount": 80
              |          },
              |          {
              |            "normalizedText": "duck",
              |            "displayText": "duck",
              |            "numExamples": 3,
              |            "frequencyCount": 23
              |          },
              |          {
              |            "normalizedText": "ducks",
              |            "displayText": "ducks",
              |            "numExamples": 5,
              |            "frequencyCount": 16
              |          },
              |          {
              |            "normalizedText": "duff",
              |            "displayText": "Duff",
              |            "numExamples": 0,
              |            "frequencyCount": 3
              |          }
              |        ]
              |      },
              |      {
              |        "normalizedTarget": "кряква",
              |        "displayTarget": "кряква",
              |        "posTag": "NOUN",
              |        "confidence": 0.0613,
              |        "prefixWord": "",
              |        "backTranslations": [
              |          {
              |            "normalizedText": "duck",
              |            "displayText": "duck",
              |            "numExamples": 0,
              |            "frequencyCount": 15
              |          },
              |          {
              |            "normalizedText": "mallard",
              |            "displayText": "Mallard",
              |            "numExamples": 1,
              |            "frequencyCount": 14
              |          }
              |        ]
              |      }
              |    ]
              |  }
              |]""".stripMargin,
            HttpStatus.SC_OK

          )
        )

        val translationClient = new AzureDictionaryLookup(new DfEnricher(testClient))
        val actual = translationClient.translate(df)
        // format: off
        val expected = createDataFrame(
          finalSchema,
          Row("duck", "verb", Seq("duck"), "To quickly lower the head or body in order to prevent it from being struck by something.", Seq.empty),
          Row("duck", "verb", Seq("duck"), "To evade doing something.", Seq.empty),
          Row("duck", "noun", Seq("duck"), "An aquatic bird of the family Anatidae, having a flat bill and webbed feet.", Seq("утка" ,"Дак" ,"Утиная" ,"уток" ,"утенок" ,"кряква")),
          Row("duck", "noun", Seq("duck"), "The flesh of a duck used as food.", Seq("утка" ,"Дак" ,"Утиная" ,"уток" ,"утенок" ,"кряква")),
        )
        // format: on

        assertDataFrameDataInColumnsEqual(actual, expected)
      }
    }
    it("should populate translations column with an empty list if no translation was found for a word") {
      // format: off
      val df = createDataFrame(
        sourceSchema,
        Row("because", "conjunction", Seq("Because"), "So that, in order that."),
      )
      // format: on

      val testClient = new TestAzureDictionaryLookupEnricher(
        EnricherResponseInfo(
          "because",
          """
            |[
            |  {
            |    "normalizedSource": "because",
            |    "displaySource": "because",
            |    "translations": []
            |  }
            |]""".stripMargin,
          HttpStatus.SC_OK

        )
      )

      val translationClient = new AzureDictionaryLookup(new DfEnricher(testClient))
      val actual = translationClient.translate(df)
      // format: off
      val expected = createDataFrame(
        finalSchema,
        Row("because", "conjunction", Seq("because", "Because"), "So that, in order that.", Seq.empty),
      )
      // format: on

      assertDataFrameDataInColumnsEqual(actual, expected)
    }

    it("should add translations with unmatched type of speech to every text") {
      // format: off
      val df = createDataFrame(
        sourceSchema,
        Row("verbatim", "noun", Seq("verbatim"), "A word-for-word report of a speech."),
        Row("verbatim", "adverb", Seq("verbatim"), "Orally; verbally."),
        Row("duck", "verb", Seq("duck"), "To evade doing something."),
        Row("duck", "verb", Seq("duck"), "To quickly lower (the head) in order to prevent it from being struck by something."),
      )
      // format: on

      val testClient = new TestAzureDictionaryLookupEnricher(
        EnricherResponseInfo(
          "verbatim",
          """
            |[
            |  {
            |    "normalizedSource": "verbatim",
            |    "displaySource": "verbatim",
            |    "translations": [
            |      {
            |        "normalizedTarget": "дословно",
            |        "displayTarget": "дословно",
            |        "posTag": "ADJ",
            |        "confidence": 0.4134,
            |        "prefixWord": "",
            |        "backTranslations": [
            |          {
            |            "normalizedText": "literally",
            |            "displayText": "literally",
            |            "numExamples": 10,
            |            "frequencyCount": 77
            |          },
            |          {
            |            "normalizedText": "verbatim",
            |            "displayText": "verbatim",
            |            "numExamples": 3,
            |            "frequencyCount": 27
            |          }
            |        ]
            |      }
            |    ]
            |  }
            |]""".stripMargin,
          HttpStatus.SC_OK
        ),
        EnricherResponseInfo(
          "duck",
          """
            |[
            |  {
            |    "normalizedSource": "duck",
            |    "displaySource": "duck",
            |    "translations": [
            |      {
            |        "normalizedTarget": "утка",
            |        "displayTarget": "утка",
            |        "posTag": "NOUN",
            |        "confidence": 0.3795,
            |        "prefixWord": "",
            |        "backTranslations": [
            |          {
            |            "normalizedText": "duck",
            |            "displayText": "duck",
            |            "numExamples": 15,
            |            "frequencyCount": 948
            |          },
            |          {
            |            "normalizedText": "weft",
            |            "displayText": "weft",
            |            "numExamples": 0,
            |            "frequencyCount": 12
            |          }
            |        ]
            |      },
            |      {
            |        "normalizedTarget": "дак",
            |        "displayTarget": "Дак",
            |        "posTag": "NOUN",
            |        "confidence": 0.2098,
            |        "prefixWord": "",
            |        "backTranslations": [
            |          {
            |            "normalizedText": "duck",
            |            "displayText": "duck",
            |            "numExamples": 15,
            |            "frequencyCount": 303
            |          },
            |          {
            |            "normalizedText": "dak",
            |            "displayText": "dak",
            |            "numExamples": 2,
            |            "frequencyCount": 35
            |          },
            |          {
            |            "normalizedText": "dac",
            |            "displayText": "DAC",
            |            "numExamples": 4,
            |            "frequencyCount": 8
            |          }
            |        ]
            |      }
            |    ]
            |  }
            |]""".stripMargin,
          HttpStatus.SC_OK

        )
      )

      val translationClient = new AzureDictionaryLookup(new DfEnricher(testClient))
      val actual = translationClient.translate(df)
      // format: off
      val expected = createDataFrame(
        finalSchema,
        Row("verbatim", "noun", Seq("verbatim"), "A word-for-word report of a speech.", Seq("дословно")),
        Row("verbatim", "adverb", Seq("verbatim"), "Orally; verbally.", Seq("дословно")),
        Row("duck", "verb", Seq("duck"), "To evade doing something.", Seq("утка", "Дак")),
        Row("duck", "verb", Seq("duck"), "To quickly lower (the head) in order to prevent it from being struck by something.", Seq("утка", "Дак")),
      )
      // format: on

      assertDataFrameDataInColumnsEqual(actual, expected)
    }

    it("Should add display name for the requested text to a form sets for all text definitions") {
      // format: off
      val df = createDataFrame(
        sourceSchema,
        Row("verbatim", "adjective", Seq("verbatim"), "(of a person) Able to take down a speech word for word, especially in shorthand."),
        Row("because", "conjunction", Seq("Because"), "So that, in order that."),
        Row("duck", "verb", Seq("DUCK", "Duck"), "To evade doing something."),
        Row("duck", "noun", Seq("DUCK", "Duck"), "The flesh of a duck used as food."),
      )
      // format: on

      val testClient = new TestAzureDictionaryLookupEnricher(
        EnricherResponseInfo(
          "verbatim",
          """
            |[
            |  {
            |    "normalizedSource": "verbatim",
            |    "displaySource": "verbatim",
            |    "translations": [
            |      {
            |        "normalizedTarget": "дословно",
            |        "displayTarget": "дословно",
            |        "posTag": "ADJ",
            |        "confidence": 0.4134,
            |        "prefixWord": "",
            |        "backTranslations": [
            |          {
            |            "normalizedText": "literally",
            |            "displayText": "literally",
            |            "numExamples": 10,
            |            "frequencyCount": 77
            |          },
            |          {
            |            "normalizedText": "verbatim",
            |            "displayText": "verbatim",
            |            "numExamples": 3,
            |            "frequencyCount": 27
            |          }
            |        ]
            |      }
            |    ]
            |  }
            |]""".stripMargin,
          HttpStatus.SC_OK
        ),
        EnricherResponseInfo(
          "because",
          """
            |[
            |  {
            |    "normalizedSource": "because",
            |    "displaySource": "because",
            |    "translations": []
            |  }
            |]""".stripMargin,
          HttpStatus.SC_OK
        ),
        EnricherResponseInfo(
          "duck",
          """
            |[
            |  {
            |    "normalizedSource": "duck",
            |    "displaySource": "duck",
            |    "translations": [
            |      {
            |        "normalizedTarget": "утка",
            |        "displayTarget": "утка",
            |        "posTag": "NOUN",
            |        "confidence": 0.3795,
            |        "prefixWord": "",
            |        "backTranslations": [
            |          {
            |            "normalizedText": "duck",
            |            "displayText": "duck",
            |            "numExamples": 15,
            |            "frequencyCount": 948
            |          },
            |          {
            |            "normalizedText": "weft",
            |            "displayText": "weft",
            |            "numExamples": 0,
            |            "frequencyCount": 12
            |          }
            |        ]
            |      }
            |    ]
            |  }
            |]""".stripMargin,
          HttpStatus.SC_OK
        )
      )

      val translationClient = new AzureDictionaryLookup(new DfEnricher(testClient))
      val actual = translationClient.translate(df)
      // format: off
      val expected = createDataFrame(
        finalSchema,
        Row("verbatim", "adjective", Seq("verbatim"), "(of a person) Able to take down a speech word for word, especially in shorthand.", Seq("дословно")),
        Row("because", "conjunction", Seq("because", "Because"), "So that, in order that.", Seq.empty),
        Row("duck", "verb", Seq("duck", "DUCK", "Duck"), "To evade doing something.", Seq.empty),
        Row("duck", "noun", Seq("duck", "DUCK", "Duck"), "The flesh of a duck used as food.", Seq("утка")),
      )
      // format: on

      assertDataFrameDataInColumnsEqual(actual, expected)
    }
  }
  describe("Should abort data enrichment, throwing an exception") {

    it("when API response with error code and an empty body") {
      val df = createDataFrame(
        sourceSchema,
        Row("duck", "noun", Seq("duck"), "The flesh of a duck used as food."),
      )
      val testClient = new TestAzureDictionaryLookupEnricher(
        EnricherResponseInfo(
          "duck", "", HttpStatus.SC_SERVICE_UNAVAILABLE
        )
      )
      val translationClient = new AzureDictionaryLookup(new DfEnricher(testClient))
      val actual = the[SparkException] thrownBy translationClient.translate(df).collect()
      val actualCause = assertCausedBy[RemoteHttpEnrichmentException](actual)
      actualCause.getMessage should include("empty body")
    }

    it("when API response with error code and an unparsable Azure error") {
      val df = createDataFrame(
        sourceSchema,
        Row("duck", "noun", Seq("duck"), "The flesh of a duck used as food."),
      )
      val errorString = "The resource is moved permanently"
      val testClient = new TestAzureDictionaryLookupEnricher(
        EnricherResponseInfo(
          "duck", errorString, HttpStatus.SC_MOVED_PERMANENTLY
        )
      )
      val translationClient = new AzureDictionaryLookup(new DfEnricher(testClient))
      val actual = the[SparkException] thrownBy translationClient.translate(df).collect()
      val actualCause = assertCausedBy[RemoteHttpEnrichmentException](actual)
      actualCause.getMessage should include(s"`${errorString}`")
    }

    it("when API response with `40000` Azure error") {
      val df = createDataFrame(
        sourceSchema,
        Row("duck", "noun", Seq("duck"), "The flesh of a duck used as food."),
      )
      val azureErrorCode = 403001
      val azureErrorMessage = "The operation isn't allowed because the subscription has exceeded its free quota."
      val testClient = new TestAzureDictionaryLookupEnricher(
        EnricherResponseInfo(
          "duck",
          s"""
             |{
             |  "error": {
             |    "code":$azureErrorCode,
             |    "message":"$azureErrorMessage"
             |    }
             |}
             |""".stripMargin,
          HttpStatus.SC_NOT_FOUND
        )
      )
      val translationClient = new AzureDictionaryLookup(new DfEnricher(testClient))
      val actual = the[SparkException] thrownBy translationClient.translate(df).collect()
      val actualCause = assertCausedBy[RemoteHttpEnrichmentException](actual)
      actualCause.getMessage should include(s"`$azureErrorCode`")
      actualCause.getMessage should include(s"`$azureErrorMessage`")
    }

    it(s"when API response with `50000` Azure error after `${MAX_RETRIES}` retries of `$AZURE_SERVICE_ERROR_RETRY_INTERVAL` retry interval") {
      val df = createDataFrame(
        sourceSchema,
        Row("duck", "noun", Seq("duck"), "The flesh of a duck used as food."),
      )
      val azureErrorCode = 503000
      val azureErrorMessage = "Service is temporarily unavailable. Retry. If the error persists, report it with date/time of error, request identifier from response header X-RequestId, and client identifier from request header X-ClientTraceId."
      val testClient = new TestAzureDictionaryLookupEnricher(
        EnricherResponseInfo(
          "duck",
          s"""
             |{
             |  "error": {
             |    "code":$azureErrorCode,
             |    "message":"$azureErrorMessage"
             |    }
             |}
             |""".stripMargin,
          HttpStatus.SC_MOVED_PERMANENTLY
        )
      )
      val translationClient = new AzureDictionaryLookup(new DfEnricher(testClient))

      val startTime = System.currentTimeMillis()
      val actual = the[SparkException] thrownBy translationClient.translate(df).collect()
      val endTime = System.currentTimeMillis()
      val elapsedTime = endTime - startTime

      val actualCause = assertCausedBy[RemoteHttpEnrichmentException](actual)
      actualCause.getMessage should include(s"`$azureErrorCode`")
      actualCause.getMessage should include(s"`$azureErrorMessage`")
      actualCause.getMessage should include(s"`$MAX_RETRIES` retries")
      Mockito.verify(testClient.httpClient, Mockito.atMost(MAX_RETRIES)).execute(ArgumentMatchers.any())

      elapsedTime should be > (MAX_RETRIES * AZURE_SERVICE_ERROR_RETRY_INTERVAL).toLong

    }
  }
}

// NOTE!!! DataFrame asserts hangs on rdd.unpersist step if `TestAzureDictionaryLookupEnricher` throw an exception.
// The reason is unknown. This does not happen with `DictionaryApiDevWordDefiner`, nor it was fixed
// by modifying the DAG in any ways. The issue is in unpersist call, so real run is not affected.
// In case execution hangs for several minutes, app must be manually stopped.
class TestAzureDictionaryLookupEnricher(val responsesInfo: EnricherResponseInfo*)
  extends AzureDictionaryLookupEnricher("DUMMY_SERVICE_KEY", "en", "ru")(None) with MockedHttpClient[String, Row] {

  import org.scalatest.Assertions._

  override def generateResponse(request: HttpUriRequest): HttpResponse = {
    import io.circe.generic.auto._
    import io.circe.parser.{decode => jsonDecode}

    val postRequest = request.asInstanceOf[HttpPost]
    val requestBody = EntityUtils.toString(postRequest.getEntity, StandardCharsets.UTF_8)
    val dictionaryLookupRequests = jsonDecode[Seq[DictionaryLookupRequest]](requestBody).right.get
    // TODO: hang issues

    assert(dictionaryLookupRequests.size === 1, "Service must send a single text entry in each request.")
    val requestedText = dictionaryLookupRequests.head.Text
    expectedHttpResponses.getOrElse(requestedText, fail(s"Requested unexpected text `${dictionaryLookupRequests}`."))
  }
}