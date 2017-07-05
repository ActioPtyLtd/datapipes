package DataSources

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.ByteBuffer
import java.nio.file._
import java.util.regex.Pattern

import DataPipes.Common.Data._
import DataPipes.Common._
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer
import boopickle.Default._

class DumpDataSource extends DataSource {

  private val logger = Logger("DumpDataSource")
  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def subscribe(observer: Observer[DataSet]): Unit = _observer.append(observer)

  def executeQuery(config: DataSet, query: DataSet): Unit = {

    val filePaths = getFilePath(config, query)

    if(filePaths.isEmpty)
      logger.warn("No files matched regex expression.")
    else {
      logger.info(s"Files matching regex expression:")
      logger.info(filePaths.mkString(","))
    }

    filePaths.foreach { filePath =>

      logger.info(s"Reading file: ${filePath}...")

      val path = Paths.get(filePath)
      val bytes = Files.readAllBytes(path)
      val bb = ByteBuffer.wrap(new Array[Byte](bytes.length))
      bb.put(bytes)
      bb.flip()

      implicit val dsPickler = compositePickler[DataSet]

      dsPickler
        .addConcreteType[DataString]
        .addConcreteType[DataBoolean]
        .addConcreteType[DataNothing]
        .addConcreteType[DataRecord]
        .addConcreteType[DataArray]
        .addConcreteType[DataDate]
        .addConcreteType[DataNumeric]

      val ds = Unpickle[DataSet].fromBytes(bb)

      logger.info(s"Completed reading file: ${filePath}...")

      logger.info(s"Deleting file: ${filePath}...")
      Files.delete(path)
      logger.info(s"Successfully deleted file: ${filePath}...")

      _observer.foreach(s => s.next(ds))
    }
  }

  def execute(config: DataSet, query: DataSet*): Unit = {

    executeQuery(config, query.headOption.getOrElse(DataNothing()))

    _observer.foreach(o => o.completed())
  }

  def getFilePath(config: DataSet, query: DataSet): List[String] = {
    val dir = config("directory").stringOption.getOrElse("")
    val regex = query("regex").stringOption
      .getOrElse(config("regex").stringOption.getOrElse(""))

    new File(dir).listFiles.filter(f => Pattern.compile(regex).matcher(f.getName).matches).map(m => m.getPath()).toList
  }

}
