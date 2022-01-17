package pb.dictionary.extraction

object CsvFilesOptions {
  private val CsvOptions = Map(
    "header" -> "true"
  )
  val WriteCsvOptions = CsvOptions
  val ReadCsvOptions  = CsvOptions ++ Map("enforceSchema" -> "false", "mode" -> "FAILFAST", "multiline" -> "true")

}
