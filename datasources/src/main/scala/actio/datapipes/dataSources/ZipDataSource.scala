package actio.datapipes.dataSources

import java.io.{File, FileInputStream, FileOutputStream}

import actio.common.Data._
import actio.common.{DataSource, Observer}
import com.typesafe.scalalogging.Logger
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.utils.IOUtils

import scala.collection.mutable.ListBuffer

class ZipDataSource extends DataSource {
  private val logger = Logger("ZipDataSource")
  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def executeQuery(config: DataSet, query: DataSet): Unit = {
    val file = new File(query("path").toString)
    val extractPath = new File(query("extractPath").toString).toString

    val tis = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)))

    var te: TarArchiveEntry = null

    while( {te = tis.getNextTarEntry; te != null} ) {
      if (!te.isDirectory) {
        val curfile = new File(extractPath + "/" + te.getName)
        val parent = curfile.getParentFile

        if (!parent.exists)
          parent.mkdirs

        IOUtils.copy(tis, new FileOutputStream(curfile))
        _observer.foreach(o => o.next(
          DataRecord(
            DataString("path", curfile.getAbsolutePath),
            DataString("name", curfile.getName),
            DataNumeric("length", curfile.length())
          )
        ))
      }
    }
    tis.close()
    _observer.foreach(o => o.completed())
  }

  def executeWrite(config: DataSet, queries: Seq[DataSet]): Unit = {

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