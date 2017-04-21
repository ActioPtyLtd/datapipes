package DataSources

import Common._
import java.io.FileReader

import Data.{DataArray, DataRecord, DataString}
import org.apache.commons.csv.{CSVFormat, CSVParser}

class FileDataSource extends DataSource {

  var fileName = "/home/maurice/gnm/frames_catalogue.csv"
  var parser: Option[CSVParser] = None

  def batchSize: Int = 10

  def execute(label: String, data: Data): DataSet = ???

  def read(data: Data): DataSet = {
    val in = new FileReader(fileName)
    parser = Some(CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in))

    new DataSourceDataSet(this)
  }

  def create(data: Data): DataSet = ???

  def update(data: Data): DataSet = ???

  def delete(data: Data): DataSet = ???

  def headOption(): Option[Data] = {
    import collection.JavaConverters._

    parser.flatMap { p =>
      val it = p.asScala.grouped(batchSize)

      if (it.hasNext)
        Some(DataArray(it.next().map(r =>
          DataRecord(r.toMap.asScala.map(c => DataString(c._1, c._2)).toList)).toList))
      else {

        None
      }
    }
  }
}
