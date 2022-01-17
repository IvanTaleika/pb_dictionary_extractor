package pb.dictionary.extraction.device

case class DeviceHighlight(
    oid: Long,
    `val`: String,
    title: String,
    authors: String,
    timeEdt: Long,
)

object DeviceHighlight {
  val OID      = "oid"
  val VAL      = "val"
  val TITLE    = "title"
  val AUTHORS  = "authors"
  val TIME_EDT = "timeEdt"
}
