package DataSources

import Common._
import java.io.FileReader

import _root_.Data.{DataArray, DataNothing, DataRecord, DataString}
import org.apache.commons.csv.{CSVFormat, CSVParser}

class CSVDataSource extends DataSource {

  def exec(parameters: Parameters): DataSet = new DataSet {
    override def subscribe(observer: Observer[DataEnvelope]): Unit = {
      run(observer, parameters)
    }
  }

  def run(observer: Observer[DataEnvelope], parameters: Parameters): Unit = {
    import collection.JavaConverters._

    val filePath = parameters("filePath").stringOption.getOrElse("")
    val batchSize: Int = 10
    val in = new FileReader(filePath)
    val parser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in)
    val it = parser.asScala.grouped(batchSize)

    it.foreach { i =>
      observer.next(DataEnvelope(DataArray(i.map(r =>
        DataRecord(r.toMap.asScala.map(c => DataString(c._1, c._2)).toList)).toList), DataNothing(), DataNothing()))
    }
    observer.completed()
  }
}
