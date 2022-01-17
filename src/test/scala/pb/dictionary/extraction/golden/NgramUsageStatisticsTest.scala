package pb.dictionary.extraction.golden

import org.apache.http.{HttpStatus, HttpVersion}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpUriRequest}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.message.BasicStatusLine
import org.apache.spark.sql.Row
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.invocation.InvocationOnMock
import pb.dictionary.extraction.TestBase
import pb.dictionary.extraction.golden.NgramUsageStatistics.NgramDfEnricher

class NgramUsageStatisticsTest extends TestBase {
  import pb.dictionary.extraction.golden.DictionaryRecord._

  val sourceSchema = s"$NORMALIZED_TEXT String, $PART_OF_SPEECH String, $OCCURRENCES Int"
  val finalSchema  = s"$sourceSchema, $USAGE double"

  describe("enrich method") {

    describe("Should populate usage column with books usage average over the requested period") {
      it("when only a single form was found for a text") {
        val df = createDataFrame(
          sourceSchema,
          Row("duck", "noun", 1),
          Row("duck", "verb", 2)
        )
        val testClient = new TestNgramRemoteHttpEnricher(
          HttpStatus.SC_OK,
          Map(
            "https://books.google.com/ngrams/json?content=duck_NOUN%3Aeng_2019&year_start=2015&year_end=2019" -> """[{"ngram": "duck_NOUN:eng_2019", "parent": "", "type": "NGRAM", "timeseries": [0.1, 0.2, 0.3, 0.4]}]""",
            "https://books.google.com/ngrams/json?content=duck_VERB%3Aeng_2019&year_start=2015&year_end=2019" -> """[{"ngram": "duck_VERB:eng_2019", "parent": "", "type": "NGRAM", "timeseries": [0.01, 0.02, 0.03, 0.04]}]""",
          )
        )

        val ngramClient = new NgramUsageStatistics(new NgramDfEnricher(testClient))
        val actual      = ngramClient.findUsageFrequency(df)
        val expected = createDataFrame(
          finalSchema,
          Row("duck", "noun", 1, 0.25),
          Row("duck", "verb", 2, 0.025)
        )
        assertDataFrameApproximateEquals(actual.orderBy(OCCURRENCES), expected, 0.001)
      }

      it("when multiple forms were found for a text") {
        val df = createDataFrame(
          sourceSchema,
          Row("Univercity of *", null, 1)
        )

        val testClient = new TestNgramRemoteHttpEnricher(
          HttpStatus.SC_OK,
          Map(
            "https://books.google.com/ngrams/json?content=Univercity+of+*%3Aeng_2019&year_start=2015&year_end=2019" ->
              """
                |[
                |  {
                |    "ngram": "Univercity of California:eng_2019",
                |    "parent": "",
                |    "type": "NGRAM",
                |    "timeseries": [
                |      0.1,
                |      0.2,
                |      0.3,
                |      0.4
                |    ]
                |  },
                |  {
                |    "ngram": "Univercity of Tokyo:eng_2019",
                |    "parent": "",
                |    "type": "NGRAM",
                |    "timeseries": [
                |      0.5,
                |      0.6,
                |      0.7,
                |      0.8
                |    ]
                |  }
                |]
              """.stripMargin,
          )
        )

        val ngramClient = new NgramUsageStatistics(new NgramDfEnricher(testClient))
        val actual      = ngramClient.findUsageFrequency(df)
        val expected = createDataFrame(
          finalSchema,
          Row("Univercity of *", null, 1, 0.45)
        )
        assertDataFrameApproximateEquals(actual.orderBy(OCCURRENCES), expected, 0.001)
      }
    }

    describe("Should populate usage column with nulls") {
      it("When no usage was found for a text") {
        val df = createDataFrame(
          sourceSchema,
          Row("lolkek", "noun", 1),
          Row("lolkek", "abbreviation", 2)
        )
        val testClient = new TestNgramRemoteHttpEnricher(
          HttpStatus.SC_OK,
          Map(
            "https://books.google.com/ngrams/json?content=lolkek_NOUN%3Aeng_2019&year_start=2015&year_end=2019" -> "[]",
            "https://books.google.com/ngrams/json?content=lolkek%3Aeng_2019&year_start=2015&year_end=2019"      -> "[]",
          )
        )

        val ngramClient = new NgramUsageStatistics(new NgramDfEnricher(testClient))
        val actual      = ngramClient.findUsageFrequency(df)
        val expected = createDataFrame(
          finalSchema,
          Row("lolkek", "noun", 1, null),
          Row("lolkek", "abbreviation", 2, null)
        )
        assertDataFrameDataEquals(actual, expected)
      }
      it("when API response with error code") {
        val df = createDataFrame(
          sourceSchema,
          Row("duck", "noun", 1),
          Row("duck", "verb", 2)
        )
        val testClient = new TestNgramRemoteHttpEnricher(
          HttpStatus.SC_SERVICE_UNAVAILABLE,
          Map(
            "https://books.google.com/ngrams/json?content=duck_NOUN%3Aeng_2019&year_start=2015&year_end=2019" -> "",
            "https://books.google.com/ngrams/json?content=duck_VERB%3Aeng_2019&year_start=2015&year_end=2019" -> "",
          )
        )

        val ngramClient = new NgramUsageStatistics(new NgramDfEnricher(testClient))
        val actual      = ngramClient.findUsageFrequency(df)
        val expected = createDataFrame(
          finalSchema,
          Row("duck", "noun", 1, null),
          Row("duck", "verb", 2, null)
        )
        assertDataFrameApproximateEquals(actual.orderBy(OCCURRENCES), expected, 0.001)
      }
    }

    it(
      s"Should make a single request for all the records with the same `$NORMALIZED_TEXT` and `$PART_OF_SPEECH` attributes") {
      val df = createDataFrame(
        sourceSchema,
        Row("duck", "noun", 1),
        Row("duck", "noun", 2),
      )
      val testClient = new TestNgramRemoteHttpEnricher(
        HttpStatus.SC_OK,
        Map(
          "https://books.google.com/ngrams/json?content=duck_NOUN%3Aeng_2019&year_start=2015&year_end=2019" -> """[{"ngram": "duck_NOUN:eng_2019", "parent": "", "type": "NGRAM", "timeseries": [0.1, 0.2, 0.3, 0.4]}]""",
        )
      )

      val ngramClient = new NgramUsageStatistics(new NgramDfEnricher(testClient))
      val actual      = ngramClient.findUsageFrequency(df)
      val expected = createDataFrame(
        finalSchema,
        Row("duck", "noun", 1, 0.25),
        Row("duck", "noun", 2, 0.25),
      )
      assertDataFrameApproximateEquals(actual.orderBy(OCCURRENCES), expected, 0.001)
      Mockito.verify(testClient.httpClient, Mockito.atMostOnce())
    }

    it("Should use part_of_speech to distinguish text meanings") {
      val df = createDataFrame(
        sourceSchema,
        Row("funny", "noun", 1),
        Row("funny", "adjective", 2),
      )
      val testClient = new TestNgramRemoteHttpEnricher(
        HttpStatus.SC_OK,
        Map(
          "https://books.google.com/ngrams/json?content=funny_NOUN%3Aeng_2019&year_start=2015&year_end=2019" -> """[{"ngram": "funny_NOUN:eng_2019", "parent": "", "type": "NGRAM", "timeseries": [0.01, 0.02, 0.03, 0.04]}]""",
          "https://books.google.com/ngrams/json?content=funny_ADJ%3Aeng_2019&year_start=2015&year_end=2019"  -> """[{"ngram": "funny_ADJ:eng_2019", "parent": "", "type": "NGRAM", "timeseries": [0.4, 0.3, 0.2, 0.1]}]""",
        )
      )

      val ngramClient = new NgramUsageStatistics(new NgramDfEnricher(testClient))
      val actual      = ngramClient.findUsageFrequency(df)
      val expected = createDataFrame(
        finalSchema,
        Row("funny", "noun", 1, 0.025),
        Row("funny", "adjective", 2, 0.25),
      )
      assertDataFrameApproximateEquals(actual.orderBy(OCCURRENCES), expected, 0.001)
    }

    it("Should omit part_of_speech filter if it is empty or unknown for the API") {
      val df = createDataFrame(
        sourceSchema,
        Row("die hard", null, 1),
        Row("MY", "abbreviation", 2),
      )
      val testClient = new TestNgramRemoteHttpEnricher(
        HttpStatus.SC_OK,
        Map(
          "https://books.google.com/ngrams/json?content=die+hard%3Aeng_2019&year_start=2015&year_end=2019" -> """[{"ngram": "die hard:eng_2019", "parent": "", "type": "NGRAM", "timeseries": [0.01, 0.02, 0.03, 0.04]}]""",
          "https://books.google.com/ngrams/json?content=MY%3Aeng_2019&year_start=2015&year_end=2019"       -> """[{"ngram": "MY:eng_2019", "parent": "", "type": "NGRAM", "timeseries": [0.4, 0.3, 0.2, 0.1]}]""",
        )
      )

      val ngramClient = new NgramUsageStatistics(new NgramDfEnricher(testClient))
      val actual      = ngramClient.findUsageFrequency(df)
      val expected = createDataFrame(
        finalSchema,
        Row("die hard", null, 1, 0.025),
        Row("MY", "abbreviation", 2, 0.25),
      )
      assertDataFrameApproximateEquals(actual.orderBy(OCCURRENCES), expected, 0.0001)
    }
  }
}

class TestNgramRemoteHttpEnricher(responseCode: Int, responses: Map[String, String])
    extends NgramEnricher("eng_2019", 2015, 2019)(None) {
  import org.scalatest.Assertions._
  override lazy val httpClient = {

    val httpClientMock = Mockito.mock(classOf[CloseableHttpClient])
    val statusLine     = new BasicStatusLine(HttpVersion.HTTP_1_1, responseCode, "")

    def usageResponse(body: StringEntity) = {
      val response = Mockito.mock(classOf[CloseableHttpResponse])
      Mockito.when(response.getStatusLine).thenReturn(statusLine)
      Mockito.when(response.getEntity).thenReturn(body)
      response
    }

    val httpResponses = responses.mapValues { responseBody =>
      usageResponse(new StringEntity(responseBody, ContentType.APPLICATION_JSON))
    }

    Mockito
      .when(httpClientMock.execute(ArgumentMatchers.any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val request = invocation.getArgument[HttpUriRequest](0)
        val url     = request.getURI.toString
        // TODO: Failure happens on the worker and hang the system for some reason. This blocks any possible automation
        httpResponses.getOrElse(url, fail(s"Request to unexpected URL $url. Stop the execution if spark hangs."))
      })
    httpClientMock
  }
}
