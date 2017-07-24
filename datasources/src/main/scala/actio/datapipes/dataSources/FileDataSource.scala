package actio.datapipes.dataSources

import java.io.File
import java.util.regex.Pattern

import actio.common.Data.{DataNothing, DataSet}
import actio.common.{DataSource, Observer}
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer

abstract class FileDataSource extends DataSource {
  private val logger = Logger("FileDataSource")
  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def init(config: DataSet): Unit
  def readAndSendFiles(config: DataSet, filePath: String): Iterable[DataSet]

  def executeQuery(config: DataSet, query: DataSet): Unit = {
    val cleanupAfterRead = !config("cleanupAfterRead").stringOption.contains("false")
    val filePaths = getFilePath(config, query)

    logger.info(s"Cleanup files after reading: ${cleanupAfterRead}")

    if (filePaths.isEmpty)
      logger.warn("No files matched regex expression.")
    else {
      logger.info(s"Files matching regex expression:")
      logger.info(filePaths.mkString(","))
    }

    val send = filePaths.foreach{ f =>
      val it = readAndSendFiles(config, f)
      it.foreach { ds =>
        _observer.foreach{ o =>
          o.next(ds)
        }
      }
    }


  }

  def execute(config: DataSet, query: DataSet*): Unit = {
    if(query.isEmpty)
      executeQuery(config, DataNothing())
    else
      query.foreach(q => executeQuery(config, q))

    _observer.foreach(o => o.completed())
  }

  def subscribe(observer: Observer[DataSet]): Unit = _observer.append(observer)

  def getFilePath(config: DataSet, query: DataSet): List[String] = {
    val dir = config("directory").stringOption.getOrElse("")
    val regex = query("regex").stringOption
      .getOrElse(config("regex").stringOption.getOrElse(query("filenameTemplate").stringOption
        .getOrElse(config("filenameTemplate").stringOption.getOrElse(""))))

    new File(dir)
      .listFiles
      .filter(f => Pattern.compile(regex).matcher(f.getName).matches)
      .sortBy(s => s.lastModified())
      .map(m => m.getPath()).toList
  }
}
