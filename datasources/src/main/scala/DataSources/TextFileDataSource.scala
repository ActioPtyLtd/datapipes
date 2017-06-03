package DataSources

import java.io.{BufferedWriter, FileReader, FileWriter}

import DataPipes.Common.Data._
import DataPipes.Common._
import org.apache.commons.csv.CSVFormat

import scala.collection.mutable.ListBuffer

class TextFileDataSource extends DataSource {

  val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def subscribe(observer: Observer[DataSet]): Unit = _observer.append(observer)

  def execute(config: DataSet, query: DataSet): Unit = {

    val filePath = query("filenameTemplate").stringOption
      .getOrElse(config("filenameTemplate").stringOption.get)

    val send = for {
      line <- io.Source.fromFile(filePath).getLines()
      o <- _observer
    } yield (line, o)

    send.foreach(s => s._2.next(DataString(s._1)))

    _observer.foreach(o => o.completed())
  }

  def execute(config: DataSet, query: DataSet*): Unit = {

    if(query.nonEmpty && !query.map(_.label).contains("read")) {

      val filePath = query.head("filenameTemplate").stringOption.getOrElse("")
      val lines = query.map(_ ("line").stringOption.getOrElse(""))

      val fw = new FileWriter(filePath, true)
      val bw = new BufferedWriter(fw)

      lines.foreach(l => {
        bw.write(l); bw.newLine()
      })

      fw.flush()
      bw.close()
      fw.close()
    } else {
      execute(config, query.headOption.getOrElse(DataNothing()))
    }
    _observer.foreach(o => o.completed())
  }
}
