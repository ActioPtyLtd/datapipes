package actio.datapipes.dataSources

import java.io.{InputStream}

import actio.common.Data._
import actio.common.{Observer}
import com.linuxense.javadbf._

object DBFDataSource {

  def read(stream: InputStream, query: DataSet, observer: Observer[DataSet]): Unit = {
    val reader = new DBFReader(stream)
    val selectFields = query("fields").map(s => s.stringOption.getOrElse(""))

    val fields = (0 until reader.getFieldCount)
      .map(i => (reader.getField(i), i))
      .filter(f => selectFields.isEmpty || selectFields.contains(f._1.getName))
      .toList

    var row = reader.nextRecord()

    while (row != null) {
      val ds = DBFDataSource.field2ds(row, fields)
      observer.next(ds)
      row = reader.nextRecord()
    }

    reader.close()
  }

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