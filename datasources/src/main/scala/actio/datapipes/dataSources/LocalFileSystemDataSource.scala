package actio.datapipes.dataSources

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Files

import actio.common.Data.{DataNothing, DataSet}
import actio.common.{DataSource, Observer}
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.FileUtils

import scala.collection.mutable.ListBuffer

class LocalFileSystemDataSource(format: String) extends DataSource {
  private val logger = Logger("LocalFileSystemDataSource")
  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def executeQuery(config: DataSet, query: DataSet): Unit = {
    val dir = config("directory").toString
    val cleanupAfterRead = !config("cleanupAfterRead").stringOption.contains("false")

    val filePaths = new File(dir)
      .listFiles.toList
      .map(_.getPath)

    val files = FileDataSource.getFilePaths(config, query, filePaths)

    logger.info(s"Cleanup files after reading: ${cleanupAfterRead}")
    if(files.isEmpty)
      logger.warn("No files matched regex expression.")
    else {
      logger.info(s"Files matching regex expression:")
      logger.info(files.mkString(","))
    }

    files.foreach { f =>
      val file = new File(f)
      val stream = new FileInputStream(file)
      _observer.foreach(o => FileDataSource.readData(stream, format, query, o))

      stream.close()

      /*if(cleanupAfterRead) {
        logger.info(s"Deleting file: ${filePath}...")
        Files.delete(path)
        logger.info(s"Successfully deleted file: ${filePath}...")
      } */
    }

    _observer.foreach(o => o.completed())
  }

  def executeWrite(config: DataSet, queries: Seq[DataSet]): Unit = {
    val dir = config("directory").toString
    val filePath = dir + "/" + queries.head("regex").stringOption
      .getOrElse(config("regex").stringOption.getOrElse(queries.head("filenameTemplate").stringOption
        .getOrElse(config("filenameTemplate").stringOption.getOrElse(""))))

    FileUtils.forceMkdir(new File(dir))

    logger.info(s"Writing to file: ${filePath}...")

    val stream = new FileOutputStream(filePath, true)

    FileDataSource.writeData(stream, format, config("compression").stringOption, queries)
    stream.close()

    logger.info(s"Completed writing to file: ${filePath}.")

    _observer.foreach(o => o.completed())
  }


  override def execute(config: DataSet, query: DataSet*): Unit = {
    if(query.isEmpty)
      executeQuery(config, DataNothing())
    else if(query.head.label == "create")
      executeWrite(config, query)
    else
      query.foreach(q => executeQuery(config, q))
  }

  def subscribe(observer: Observer[DataSet]): Unit = _observer.append(observer)
}
