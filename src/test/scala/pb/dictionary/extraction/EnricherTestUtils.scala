package pb.dictionary.extraction

import org.apache.http.{HttpResponse, HttpVersion}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpUriRequest}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.message.BasicStatusLine
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.invocation.InvocationOnMock

object EnricherTestUtils {

  case class EnricherResponseInfo(requestKey: String, responseBody: String, responseCode: Int)

  // Must be kept outside of Scalatest suites to be serializable
  trait MockedHttpClient[In, Out] extends RemoteHttpEnricher[In, Out] {

    def responsesInfo: Seq[EnricherResponseInfo]
    def generateResponse(request: HttpUriRequest): HttpResponse

    def expectedHttpResponses: Map[String, CloseableHttpResponse] =
      responsesInfo.map(ri => ri.requestKey -> buildHttpResponse(ri)).toMap

    private def buildHttpResponse(responseInfo: EnricherResponseInfo) = {
      val statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1, responseInfo.responseCode, "")
      val body       = new StringEntity(responseInfo.responseBody, ContentType.APPLICATION_JSON)
      val response   = Mockito.mock(classOf[CloseableHttpResponse])
      Mockito.when(response.getStatusLine).thenReturn(statusLine)
      Mockito.when(response.getEntity).thenReturn(body)
      response
    }

    override lazy val httpClient = {
      val httpClientMock = Mockito.mock(classOf[CloseableHttpClient])

      Mockito
        .when(httpClientMock.execute(ArgumentMatchers.any()))
        .thenAnswer((invocation: InvocationOnMock) => {
          val request = invocation.getArgument[HttpUriRequest](0)
          generateResponse(request)
        })
      httpClientMock
    }
  }
}
