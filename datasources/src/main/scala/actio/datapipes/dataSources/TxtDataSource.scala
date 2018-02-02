package actio.datapipes.dataSources

import java.io._

import actio.common.Data.{DataNothing, DataRecord, DataSet, DataString}
import actio.common.Observer

object TxtDataSource {

  def read(stream: InputStream, observer: Observer[DataSet]): Unit = {
    scala.io.Source.fromInputStream(stream).getLines().foreach{ line =>
      observer.next(DataRecord(DataString("line", line)))
    }
  }

  def write(stream: OutputStream, queries: Seq[DataSet]): Unit = {
    val osw = new OutputStreamWriter(stream)
    val bw = new BufferedWriter(osw)

    val lines = queries.map(q => q("line").stringOption.getOrElse(q(0).stringOption.getOrElse("")))

    lines.foreach(l => {
      bw.write(l); bw.newLine()
    })

    osw.flush()
    bw.close()
    osw.close()
  }
}
