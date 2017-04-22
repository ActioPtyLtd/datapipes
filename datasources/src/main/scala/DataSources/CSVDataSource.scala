package DataSources

import Common._
import java.io.FileReader

import Data.{DataArray, DataRecord, DataString}
import org.apache.commons.csv.{CSVFormat, CSVParser}

class CSVDataSource extends DataSource {

  var fileName = "/home/maurice/gnm/frames_catalogue.csv"
  var parser: Option[CSVParser] = None

  val batchSize: Int = 10

  def execute(label: String, data: Data): DataSet = {
    val in = new FileReader(fileName)
    parser = Some(CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in))

    new DataSourceDataSet(this)
  }

  def headOption(): Option[Data] = {
    import collection.JavaConverters._

    parser.flatMap { p =>
      val it = p.asScala.grouped(batchSize)

      if (it.hasNext)
        Some(DataArray(it.next().map(r =>
          DataRecord(r.toMap.asScala.map(c => DataString(c._1, c._2)).toList)).toList))
      else {
        // TODO: close the file reader
        None
      }
    }
  }
}
