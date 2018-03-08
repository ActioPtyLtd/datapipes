package actio.datapipes.dataSources

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Path}

import actio.common.Data.{DataNothing, DataSet}
import actio.common.{DataSource, Observer}
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.FileUtils

import scala.collection.mutable.ListBuffer

class LocalFileSystemDataSource(format: String) extends DataSource {
  private val logger = Logger("LocalFileSystemDataSource")
  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def executeQuery(config: DataSet, query: DataSet): Unit = {
    val dir = config("directory").toOption.orElse(query("directory").toOption).get.toString
    val cleanupAfterRead = config("cleanupAfterRead").stringOption.contains("true")

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

      if(cleanupAfterRead) {
        logger.info(s"Deleting file: ${f}...")
        Files.delete(new File(f).toPath)
        logger.info(s"Successfully deleted file: ${f}...")
      }
    }

    _observer.foreach(o => o.completed())
  }

  def executeWrite(config: DataSet, queries: Seq[DataSet]): Unit = {

    val fileGroups = queries.groupBy(q => (
      q("directory").stringOption.getOrElse(config("directory").toString),
      q("regex").stringOption.orElse(q("filenameTemplate").stringOption).orElse(config("regex").stringOption).getOrElse(q("filenameTemplate").toString)
    ))

    fileGroups.foreach { fileGroup =>

      // create folder if required
      FileUtils.forceMkdir(new File(fileGroup._1._1))

      val filePath = fileGroup._1._1 + "/" + fileGroup._1._2

      logger.info(s"Writing to file: ${filePath}...")

      val append = !fileGroup._2.exists(f => f("append").stringOption.contains("false"))
      val stream = new FileOutputStream(filePath, append)

      // write all lines to the appropriate file
      FileDataSource.writeData(stream, format, config("compression").stringOption, fileGroup._2)
      stream.close()

      logger.info(s"Completed writing to file: ${filePath}.")
    }

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
