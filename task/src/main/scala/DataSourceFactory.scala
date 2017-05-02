import Common.{DataSource, Parameters}
import DataSources.StdInDataSource

object DataSourceFactory {
  def create(parameters: Parameters): DataSource = {
    new StdInDataSource() // example
  }
}
