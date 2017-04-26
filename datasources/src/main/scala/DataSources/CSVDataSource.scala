package DataSources

import Common._
import java.io.FileReader
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import _root_.Data.{DataArray, DataNothing, DataRecord, DataString}
import org.apache.commons.csv.{CSVFormat, CSVParser}

class CSVDataSource extends DataSource {

  def exec(parameters: Parameters): DataSet = new DataSet {
    override def subscribe(observer: Observer[DataEnvelope]): Unit = {
      // keep observer and parameters
    }
  }

  def run(observer: Observer[DataEnvelope], parameters: Parameters): Future[Unit] = async {
    import collection.JavaConverters._

    val filePath = parameters("filePath").stringOption.getOrElse("")
    val batchSize: Int = 10
    val in = new FileReader(filePath)
    val parser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in)
    val it = parser.asScala.grouped(batchSize)

    val i = it.next()

      await { observer.next(DataEnvelope(DataArray(i.map(r =>
        DataRecord(r.toMap.asScala.map(c => DataString(c._1, c._2)).toList)).toList), DataNothing(), DataNothing())) }

    await { observer.completed() }
  }
}
