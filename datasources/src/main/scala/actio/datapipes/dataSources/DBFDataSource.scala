package actio.datapipes.dataSources

import java.io.{File, FileInputStream}

import DataPipes.Common._
import DataPipes.Common.Data._
import com.linuxense.javadbf._
import com.typesafe.scalalogging.Logger

class DBFDataSource extends DataSource {
  val logger = Logger("DBFDataSource")

  var _observer: Option[Observer[DataSet]] = None

  def execute(config: DataSet, query: DataSet): Unit = {

    val fileName = getFilePath(config, query)

    logger.info(s"Reading file: ${fileName}")

    val fis = new FileInputStream(new File(fileName))
    val stream = new DBFReader(fis)
    val selectFields = query("fields").map(s => s.stringOption.getOrElse(""))

    val fields = (0 until stream.getFieldCount)
      .map(i => (stream.getField(i),i))
      .filter(f => selectFields.isEmpty || selectFields.contains(f._1.getName))
      .toList

    var row = stream.nextRecord()

    while (row != null) {
      val ds = DBFDataSource.field2ds(row, fields)

      if(_observer.isDefined)
        { _observer.get.next(ds) }

      row = stream.nextRecord()
    }

    if(_observer.isDefined)
      { _observer.get.completed() }

    stream.close()
    fis.close()
    logger.info(s"Completed reading file: ${fileName}")

  }

  def subscribe(observer: Observer[DataSet]): Unit = _observer = Some(observer)

  def execute(config: DataSet, query: DataSet*): Unit = {
    if(query.nonEmpty)
      query.foreach(q => execute(config, q))
    else
      execute(config, DataNothing())
  }

  def getFilePath(config: DataSet, query: DataSet): String = config("directory").stringOption.map(_ + "/").getOrElse("") + query("filenameTemplate").stringOption
    .getOrElse(config("filenameTemplate").stringOption.getOrElse(""))
}

object DBFDataSource {

  def field2ds(row: Array[Object], fields: List[(DBFField,Int)]): DataSet =
    DataRecord("row", fields.map(f => {
      val t = f._1.getType

      if (Option(row(f._2)).isDefined) {
        if (t == DBFDataType.NUMERIC || t == DBFDataType.FLOATING_POINT || t == DBFDataType.LONG || t == DBFDataType.CURRENCY) {
          DataNumeric(f._1.getName, BigDecimal(BigDecimal(row(f._2).toString).underlying()
            .stripTrailingZeros()
            .toPlainString))
        } else if (t == DBFDataType.DATE || t == DBFDataType.TIMESTAMP) {
          DataDate(f._1.getName, row(f._2).asInstanceOf[java.util.Date])
        } else {
          DataString(f._1.getName, row(f._2).toString.trim())
        }
      } else {
        DataNothing(f._1.getName)
      }
    }))
}