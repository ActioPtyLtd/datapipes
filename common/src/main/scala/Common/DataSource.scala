package Common

trait DataSource {

  def execute(label: String, data: Data): DataSet

  private[Common] def headOption(): Option[Data]
}
