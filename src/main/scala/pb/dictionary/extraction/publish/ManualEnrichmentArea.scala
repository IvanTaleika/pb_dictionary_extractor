package pb.dictionary.extraction.publish

import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.functions._
import pb.dictionary.extraction.silver.DefinedText
import pb.dictionary.extraction.{ApplicationManagedArea, CsvSnapshotsArea}

import java.sql.Timestamp
import java.time.{ZonedDateTime, ZoneOffset}

// TODO: write directly to Google Docs? Export csv programmatically? Rename output file?
// TODO: do not export words already defined in the publish
class ManualEnrichmentArea(
    path: String,
    timestampProvider: () => Timestamp = () => Timestamp.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
) extends CsvSnapshotsArea[DefinedText, UndefinedText](path, timestampProvider) {
  import UndefinedText._
  override protected def outputFiles = Option(1)

  override def upsert(silverSnapshot: Dataset[DefinedText]): Dataset[UndefinedText] = {
    silverSnapshot.transform(fromSilver).transform(writeSnapshot)
  }

  private def fromSilver(definedText: Dataset[DefinedText]) = {
    definedText
      .filter(col(DefinedText.DEFINITION).isNull)
      .select(
        col(TEXT),
        col(OCCURRENCES),
        array_join(col(BOOKS), ",") as BOOKS,
        timestampToCsvString(col(FIRST_OCCURRENCE)) as FIRST_OCCURRENCE,
        timestampToCsvString(col(LATEST_OCCURRENCE)) as LATEST_OCCURRENCE,
      )
  }
}
