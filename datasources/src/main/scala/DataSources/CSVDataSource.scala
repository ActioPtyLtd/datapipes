package DataSources

import DataPipes.Common.Data._
import DataPipes.Common._
import java.io.FileReader

import com.typesafe.scalalogging.Logger
import org.apache.commons.csv.{CSVFormat, CSVParser}

class CSVDataSource extends DataSource {

  var _observer: Option[Observer[DataSet]] = None

  def subscribe(observer: Observer[DataSet]): Unit = _observer = Some(observer)

  def execute(config: DataSet, query: DataSet): Unit = {
    import collection.JavaConverters._

    val filePath = config("filePath").stringOption.getOrElse("")

    val in = new FileReader(filePath)
    val parser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in)
    val it = parser.iterator()

    while (it.hasNext) {
      val i = it.next()

      _observer.get.next(DataRecord(i.toMap.asScala.map(c => DataString(c._1, c._2)).toList))

    }

    in.close()

    _observer.get.completed()
  }

  override def executeBatch(config: DataSet, query: Seq[DataSet]): Unit = ???
}
