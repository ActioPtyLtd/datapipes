package actio.datapipes.dataSources
import java.io.InputStream

import actio.common.Data.{DataNothing, DataSet}
import actio.common.{DataSource, Observer}
import com.typesafe.scalalogging.Logger
import org.apache.commons.net.ftp._

import scala.collection.mutable.ListBuffer

class FTPDataSource(format: String) extends DataSource {
  private val logger = Logger("FTPDataSource")
  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def executeQuery(connection: FTPClient,config: DataSet, query: DataSet): Unit = {
    val dir = config("directory").stringOption.getOrElse("")

    logger.info(s"Changing working directory to: $dir...")
    connection.changeWorkingDirectory(dir)
    logger.info(s"Successfully changed working directory to: $dir.")

    val fileNames = connection.listFiles().map(_.getName).toList
    val files = FileDataSource.getFilePaths(config, query, fileNames)

    if(files.isEmpty)
      logger.warn("No files matched regex expression.")
    else {
      logger.info(s"Files matching regex expression:")
      logger.info(files.mkString(","))
    }

    files.foreach { f =>
      val stream = connection.retrieveFileStream(f)
      _observer.foreach(o => FileDataSource.readData(stream, format, query, o))

      stream.close()
    }

    _observer.foreach(o => o.completed())
  }

  def connect(config: DataSet): FTPClient = {
    val connect = config("connect").stringOption.getOrElse("")
    val username = config("credential")("username").stringOption.getOrElse("")
    val password = config("credential")("password").stringOption.getOrElse("")
    val port = config("port").intOption

    val ftp: FTPClient = new FTPClient()
    val conf: FTPClientConfig = new FTPClientConfig()

    conf.setLenientFutureDates(true)
    ftp.configure(conf)

    logger.info(s"Connecting to: $connect:$port...")


    if (port.isEmpty)
      ftp.connect(connect)
    else
      ftp.connect(connect,port.get)

    if (!FTPReply.isPositiveCompletion(ftp.getReplyCode)) {
      ftp.disconnect()
      throw new Throwable("Failed to connect.")
    }

    ftp.login(username,password)

    if (!FTPReply.isPositiveCompletion(ftp.getReplyCode)) {
      ftp.disconnect()
      throw new Throwable("Failed to login.")
    }

    ftp.enterLocalPassiveMode()

    if (!FTPReply.isPositiveCompletion(ftp.getReplyCode)) {
      ftp.disconnect()
      throw new Throwable("FTP server failed to go into passive mode.")
    }

    logger.info(s"Successfully connected to: ${connect}.")

    ftp
  }

  def getFiles(config: DataSet, query: DataSet): List[String] = List[String]()

  override def execute(config: DataSet, query: DataSet*): Unit = {
    val ftpconn = connect(config)

    if(query.isEmpty)
      executeQuery(ftpconn, config, DataNothing())
    else
      query.foreach(q => executeQuery(ftpconn, config, q))

    ftpconn.disconnect()
  }


  def subscribe(observer: Observer[DataSet]): Unit = _observer.append(observer)
}
