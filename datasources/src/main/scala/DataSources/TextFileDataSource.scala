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

    val filePath = query("filePath").stringOption.getOrElse("")

    val send = for {
      line <- io.Source.fromFile(filePath).getLines()
      o <- _observer
    } yield (line, o)

    send.foreach(s => s._2.next(DataString(s._1)))

    _observer.foreach(o => o.completed())
  }

  def execute(config: DataSet, query: DataSet*): Unit = {

    if(query.headOption.exists(m => m("line").toOption.isDefined)) {

      val filePath = query.head("filePath").stringOption.getOrElse("")
      val lines = query.map(_ ("line").stringOption.getOrElse(""))

      val fw = new FileWriter(filePath, true)
      val bw = new BufferedWriter(fw)

      lines.foreach(l => {
        bw.write(l); bw.newLine()
      })

      fw.flush()
      bw.close()
      fw.close()
    } else if(query.nonEmpty){
      execute(config, query.headOption.get)
    }
    _observer.foreach(o => o.completed())
  }
}
