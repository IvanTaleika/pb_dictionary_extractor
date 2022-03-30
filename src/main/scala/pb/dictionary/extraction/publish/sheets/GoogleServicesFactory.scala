package pb.dictionary.extraction.publish.sheets

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient
import com.google.api.client.json.gson.GsonFactory
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import pb.dictionary.extraction.AreaUtils

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class GoogleServicesFactory(applicationName: String, credentialsFilePath: String) {
  private val JSON_FACTORY   = GsonFactory.getDefaultInstance
  private val HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport

  private def createCredentials(scopes: Seq[String]) = { // Load client secrets.
    val credentialsDescription = AreaUtils.fetchCredentialsFile(credentialsFilePath)
    try {
      val credentials = GoogleCredentials.fromStream(credentialsDescription).createScoped(scopes.asJava)
      new HttpCredentialsAdapter(credentials)
    } finally {
      credentialsDescription.close()
    }
  }

  def create[T <: AbstractGoogleJsonClient](scopes: Seq[String])(implicit classTag: ClassTag[T]): T = {
    val credentials = createCredentials(scopes)

    Class
      .forName(s"${classTag.toString()}$$Builder")
      .getConstructors
      .head
      .newInstance(HTTP_TRANSPORT, JSON_FACTORY, credentials)
      .asInstanceOf[AbstractGoogleJsonClient.Builder]
      .setApplicationName(applicationName)
      .build()
      .asInstanceOf[T]
  }

  def create[T <: AbstractGoogleJsonClient](scope: String)(implicit classTag: ClassTag[T]): T = {
    create(Seq(scope))
  }
}
