package actio.datapipes.dataSources

import java.io.{InputStream}
import java.nio.ByteBuffer
import actio.common.Data._
import actio.common.{Observer}
import boopickle.Default._
import org.apache.commons.io.IOUtils

object DumpDataSource {

  def read(stream: InputStream, observer: Observer[DataSet]): Unit = {
    val bytes = IOUtils.toByteArray(stream)
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

    ds.elems.foreach(d => observer.next(d))
  }
}
