package actio.datapipes.dataSources

import java.io.{BufferedWriter, OutputStream, OutputStreamWriter}

import actio.common.Data.{DataNothing, DataSet}

object TxtDataSource {

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
