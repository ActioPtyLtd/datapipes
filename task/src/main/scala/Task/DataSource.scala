package Task

import Common.{Parameters}
import DataSources.StdInDataSource

object DataSource {
  def create(parameters: Parameters): Common.DataSource = {
    new StdInDataSource() // example
  }
}
