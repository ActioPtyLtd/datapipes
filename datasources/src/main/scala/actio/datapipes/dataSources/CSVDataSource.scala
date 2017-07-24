package actio.datapipes.dataSources

import java.io.{InputStream, InputStreamReader}

import actio.common.Data.{DataRecord, DataSet, DataString}
import actio.common.Observer
import org.apache.commons.csv.CSVFormat

object CSVDataSource  {
  def read(stream: InputStream, observer: Observer[DataSet]): Unit = {
    import collection.JavaConverters._

    val reader = new InputStreamReader(stream)
    val parser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(reader)
    val it = parser.iterator()

    while (it.hasNext) {
      val i = it.next()

      observer.next(DataRecord(i.toMap.asScala.map(c => DataString(c._1, c._2)).toList))
    }
    reader.close()
  }
}
