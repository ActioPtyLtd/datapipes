package DataSources

import Common._
import java.io.FileReader
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import Common.Data.{DataArray, DataNothing, DataRecord, DataString}
import org.apache.commons.csv.{CSVFormat, CSVParser}

class CSVDataSource extends DataSource {

  var _observer: Option[Observer[DataSet]] = None

  def subscribe(observer: Observer[DataSet]): Unit = _observer = Some(observer)

  def exec(parameters: Parameters): Future[Unit] = async {
    import collection.JavaConverters._

    val filePath = parameters("filePath").stringOption.getOrElse("")

    val in = new FileReader(filePath)
    val parser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in)
    val it = parser.iterator()

    while (it.hasNext) {
      val i = it.next()

      await {
        _observer.get.next(DataRecord(i.toMap.asScala.map(c => DataString(c._1, c._2)).toList))
      }
    }

    in.close()

    await { _observer.get.completed() }
  }
}
