package actio.datapipes.dataSources

import java.io._
import java.util.regex.Pattern

import actio.common.Data.{DataSet}
import actio.common.Observer
import java.util.zip.GZIPOutputStream

object FileDataSource {

  // TODO sort by last modified
  def getFilePaths(config: DataSet, query: DataSet, files: List[String]): List[String] = {
    val regex = query("regex").stringOption
      .getOrElse(config("regex").stringOption.getOrElse(query("filenameTemplate").stringOption
        .getOrElse(config("filenameTemplate").stringOption.getOrElse(""))))

    files
      .filter(f => Pattern.compile(regex).matcher(new File(f).getName).matches)
      //.sortBy(s => s.lastModified())
  }

  def readData(stream: InputStream, format: String, query: DataSet, observer: Observer[DataSet]): Unit = {
    if(format == "dump") {
      DumpDataSource.read(stream, observer)
    } else if(format == "csv") {
      CSVDataSource.read(stream, observer)
    }
    else if(format == "dbf") {
      DBFDataSource.read(stream, query, observer)
    } else if (format == "json") {
      JsonDataSource.read(stream, observer)
    }
    else if(format == "txt") {
      TxtDataSource.read(stream, observer)
    }
  }

  def writeData(stream: OutputStream, format: String, compression: Option[String], queries: Seq[DataSet]): Unit = {

    val writeStream =
      if (compression.isDefined)
        new GZIPOutputStream(stream)
      else
        stream

    if(format == "txt") {
      TxtDataSource.write(writeStream, queries)
    } else if(format == "json") {
      JsonDataSource.write(writeStream, queries)
    } else if(format == "dump") {
      DumpDataSource.write(writeStream, queries)
    }

    if (compression.isDefined) {
      writeStream.close()
    }
  }

}

