package Task

import java.io.{File, FileOutputStream}

import DataPipes.Common.Data._
import DataPipes.Common._

import scala.collection.mutable.ListBuffer
import boopickle.Default._
import com.typesafe.scalalogging.Logger

class TaskFileDump(val name: String, config: DataSet) extends Task {

  private val logger = Logger("TaskFileDump")
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()

  def completed(): Unit = _observer.foreach(o => o.completed())

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val tmpFile = File.createTempFile(config.label, ".ds", new File(config("directory").stringOption.get))
    val fileOut = new FileOutputStream(tmpFile)

    implicit val dsPickler = compositePickler[DataSet]

    dsPickler
      .addConcreteType[DataString]
      .addConcreteType[DataBoolean]
      .addConcreteType[DataNothing]
      .addConcreteType[DataRecord]
      .addConcreteType[DataArray]
      .addConcreteType[DataDate]
      .addConcreteType[DataNumeric]

    logger.info(s"Writing file: ${tmpFile.getName}...")
    fileOut.write(Pickle.intoBytes(value.headOption.map(m => m.success).getOrElse(DataNothing())).array())
    fileOut.close()
    logger.info(s"Completed writing file: ${tmpFile.getName}...")

    _observer.foreach(s => s.next(value))
  }

  def subscribe(observer: Observer[Dom]): Unit = _observer.append(observer)

}