import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object App {

  // APIs worth trying (partially based on https://medium.com/@martin.breuss/finding-a-useful-dictionary-api-52084a01503d)
  // Definition:
  // 1. https://dictionaryapi.dev/
  // 2. https://developer.oxforddictionaries.com/
  // 3. https://www.dictionaryapi.com/
  // Translation:
  // 1. https://cloud.google.com/translate
  // Usage Rating:
  // 1. https://books.google.com/ngrams

  // Flow: Retrieve from db file -> parse -> filter bookmarks -> clean -> marge into delta -> extract new
  // -> define -> checkout golden delta by normalized form -> extract new -> translate -> merge into golden delta
  // -> extract as CSV (override???) -> send to google sheets ???
  // Q: will we be able to sort google sheet by the timestamp/book/usage index
  // Q: can we use https://books.google.com/ngrams/info for usage rating?

  private val Path                = "D:/system/config/books.db"
  private val HighlightTagId      = 104
  private val HighlightInfoSchema = StructType.fromDDL("begin String, end String, text String, updated Timestamp")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("pb_dictionary_extractor").getOrCreate()

    val dbDf = spark.read
      .format("jdbc")
      .option("driver", "org.sqlite.JDBC")
      .option("url", s"jdbc:sqlite:$Path")
//      .option("url", s"D:\\system\\config\\books.db")
      .option(
        "query",
        s"""
           |select
           |  t.oid,
           |  t.val,
           |  b.Title,
           |  b.Authors,
           |  t.timeEdt
           |from Tags as t
           |join items as i
           |  on t.ItemID = i.OID
           |join items as ii
           |  on i.ParentID = ii.OID
           |join books as b
           |  on ii.OID = b.OID
           |where t.TagID = $HighlightTagId""".stripMargin
      )
      .load()
    val parsedDf = dbDf
      .withColumn("parsedVal", from_json(col("val"), HighlightInfoSchema))
      // Bookmarks does not have begin/end attributes
      .where(col("parsedVal")("begin").isNotNull)
      .select(
        col("oid"),
        col("parsedVal")("text") as "text",
        col("title"),
        col("Authors"),
        col("timeEdt"),
      )

    parsedDf.show(false)

  }
}
