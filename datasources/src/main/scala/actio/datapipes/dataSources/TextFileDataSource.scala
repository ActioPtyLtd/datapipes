package actio.datapipes.dataSources

import java.io.{BufferedWriter, FileReader, FileWriter}
import java.nio.file._

import DataPipes.Common.Data._
import DataPipes.Common._
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer

class TextFileDataSource extends DataSource {

  private val logger = Logger("TextFileDataSource")
  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def subscribe(observer: Observer[DataSet]): Unit = _observer.append(observer)

  def executeQuery(config: DataSet, query: DataSet): Unit = {

    val filePath = getFilePath(config, query)

    logger.info(s"Reading file: ${filePath}...")

    val send = for {
      line <- io.Source.fromFile(filePath).getLines()
      o <- _observer
    } yield (o, line)

    send.foreach(s => s._1.next(DataString(s._2)))

    logger.info(s"Completed reading file: ${filePath}...")

    _observer.foreach(o => o.completed())
  }

  def execute(config: DataSet, query: DataSet*): Unit = {

    if (query.nonEmpty && !query.map(_.label).contains("read")) {

      val lines = query.map(q => q("line").stringOption.getOrElse(q(0).stringOption.getOrElse("")))
      val filePath = getFilePath(config, query.headOption.getOrElse(DataNothing()))
      val path = Paths.get(filePath).toAbsolutePath
      val doesFileExist = Files.exists(path)

      logger.info(s"Writing to file: ${filePath}...")
      val fw = new FileWriter(filePath, true)
      val bw = new BufferedWriter(fw)

      if (!doesFileExist) {
        config("header").stringOption.foreach(h => {
          bw.write(h); bw.newLine()
        })
      }

      lines.foreach(l => {
        bw.write(l); bw.newLine()
      })

      fw.flush()
      bw.close()
      fw.close()

      logger.info(s"Completed writing ${lines.size} lines to file: ${filePath}...")
    } else {
      executeQuery(config, query.headOption.getOrElse(DataNothing()))
    }

    _observer.foreach(o => o.completed())
  }

  def getFilePath(config: DataSet, query: DataSet): String = config("directory").stringOption.map(_ + "/").getOrElse("") + query("filenameTemplate").stringOption
    .getOrElse(config("filenameTemplate").stringOption.getOrElse(""))
}
