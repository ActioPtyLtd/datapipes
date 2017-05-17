package DataSources

import java.io.{File, FileInputStream}

import DataPipes.Common._
import DataPipes.Common.Data._
import com.linuxense.javadbf._

class DBFDataSource extends DataSource {

  var _observer: Option[Observer[DataSet]] = None

  def exec(parameters: Parameters): Unit = {

    val fileName = parameters("filePath").stringOption.getOrElse("")
    val fis = new FileInputStream(new File(fileName))
    val stream = new DBFReader(fis)
    val selectFields = parameters("fields").map(s => s.stringOption.getOrElse(""))

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

  }

  def subscribe(observer: Observer[DataSet]): Unit = _observer = Some(observer)
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