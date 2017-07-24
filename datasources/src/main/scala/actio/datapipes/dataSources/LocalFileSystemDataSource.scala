package actio.datapipes.dataSources

import java.io.{File, FileInputStream}
import java.nio.file.Files

import actio.common.Data.{DataNothing, DataSet}
import actio.common.{DataSource, Observer}
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer

class LocalFileSystemDataSource(format: String) extends DataSource {
  private val logger = Logger("LocalFileSystemDataSource")
  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def executeQuery(config: DataSet, query: DataSet): Unit = {
    val dir = config("directory").stringOption.getOrElse("")
    val cleanupAfterRead = !config("cleanupAfterRead").stringOption.contains("false")

    val filePaths = new File(dir)
      .listFiles.toList
      .map(_.getPath)

    val files = FileDataSource.getFilePaths(config, query, filePaths)

    logger.info(s"Cleanup files after reading: ${cleanupAfterRead}")
    if(filePaths.isEmpty)
      logger.warn("No files matched regex expression.")
    else {
      logger.info(s"Files matching regex expression:")
      logger.info(filePaths.mkString(","))
    }

    files.foreach { f =>
      val file = new File(f)
      val stream = new FileInputStream(file)
      _observer.foreach(o => FileDataSource.sendData(stream, format, query, o))

      stream.close()

      /*if(cleanupAfterRead) {
        logger.info(s"Deleting file: ${filePath}...")
        Files.delete(path)
        logger.info(s"Successfully deleted file: ${filePath}...")
      } */
    }

    _observer.foreach(o => o.completed())
  }

  override def execute(config: DataSet, query: DataSet*): Unit = {
    if(query.isEmpty)
      executeQuery(config, DataNothing())
    else
      query.foreach(q => executeQuery(config, q))
  }

  def subscribe(observer: Observer[DataSet]): Unit = _observer.append(observer)
}
