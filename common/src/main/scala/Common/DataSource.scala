package Common

trait DataSource {

  def batchSize: Int

  def execute(label: String, data: Data): DataSet

  def read(data: Data): DataSet

  def create(data: Data): DataSet

  def update(data: Data): DataSet

  def delete(data: Data): DataSet

  private[Common] def headOption(): Option[Data]
}
