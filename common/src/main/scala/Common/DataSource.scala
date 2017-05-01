package Common

trait DataSource {

  def exec(parameters: Parameters): Observable[DataSet]
}
