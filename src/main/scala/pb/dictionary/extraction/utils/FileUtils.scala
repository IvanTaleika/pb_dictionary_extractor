package pb.dictionary.extraction.utils

import org.apache.commons.io.IOUtils

import java.io.{File, FileInputStream, FileNotFoundException}
import java.nio.charset.StandardCharsets

object FileUtils {

  def connectToFile(path: String): FileInputStream = {
    val credentialsFile = new File(path)
    if (!credentialsFile.exists()) {
      throw new FileNotFoundException(s"Credentials file not found: ${path}")
    }
    new FileInputStream(credentialsFile)
  }

  def readToString(path: String): String = {
    val file = connectToFile(path)
    try {
      IOUtils.toString(file, StandardCharsets.UTF_8)
    } finally {
      file.close()
    }
  }

}
