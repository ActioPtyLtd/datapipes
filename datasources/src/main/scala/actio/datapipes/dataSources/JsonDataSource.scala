package actio.datapipes.dataSources

import java.io.{BufferedWriter, InputStream, OutputStream, OutputStreamWriter}

import actio.common.Data.{DataNothing, DataSet}
import actio.common.Data.JsonXmlDataSet._
import actio.common.Observer
import org.apache.commons.io.IOUtils

object JsonDataSource {
  def read(stream: InputStream, observer: Observer[DataSet]): Unit = {
    val str = IOUtils.toString(stream)
    val ds = actio.common.Data.JsonXmlDataSet.fromJson(str)
    observer.next(ds)
  }


  def write(stream: OutputStream, queries: Seq[DataSet]): Unit = {
    val osw = new OutputStreamWriter(stream)
    val bw = new BufferedWriter(osw)

    val line = queries.headOption.getOrElse(DataNothing()).toJson

      bw.write(line)

    osw.flush()
    bw.close()
    osw.close()
  }
}
