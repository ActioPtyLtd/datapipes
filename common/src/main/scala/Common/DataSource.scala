package Common

trait DataSource extends IO {

  private[Common] def headOption(): Option[Data]
}
