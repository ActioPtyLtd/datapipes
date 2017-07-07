package actio.datapipes.dataSources

import java.io.FileReader

import actio.common.Data.{DataNothing, DataRecord, DataSet, DataString}
import actio.common.{DataSource, Observer}
import org.apache.commons.csv.CSVFormat

class CSVDataSource extends DataSource {

  var _observer: Option[Observer[DataSet]] = None

  def subscribe(observer: Observer[DataSet]): Unit = _observer = Some(observer)

  def execute(config: DataSet, query: DataSet): Unit = {
    import collection.JavaConverters._

    val filePath = getFilePath(config, query)

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

  def execute(config: DataSet, query: DataSet*): Unit = {
    if(query.isEmpty)
      execute(config, DataNothing())
    else
      query.foreach(q => execute(config, q))
  }

  def getFilePath(config: DataSet, query: DataSet): String = config("directory").stringOption.map(_ + "/").getOrElse("") + query("filenameTemplate").stringOption
    .getOrElse(config("filenameTemplate").stringOption.getOrElse(""))
}
