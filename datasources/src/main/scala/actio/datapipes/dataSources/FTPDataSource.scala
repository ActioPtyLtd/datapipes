package actio.datapipes.dataSources
import java.io.InputStream

import actio.common.Data.{DataNothing, DataSet}
import com.typesafe.scalalogging.Logger
import org.apache.commons.net.ftp._

class FTPDataSource(format: String) {
  private val logger = Logger("FTPDataSource")

  var ftpconn: FTPClient = _

  def init(config: DataSet): Unit = {
    ftpconn = connect(config)
  }

  def getStream(config: DataSet, filePath: String): InputStream = {
    val fs = ftpconn.retrieveFileStream(filePath)
    ftpconn.completePendingCommand()
    ftpconn.disconnect()
    fs
  }

  def connect(config: DataSet): FTPClient = {
    val connect = config("connect").stringOption.getOrElse("")
    val username = config("username").stringOption.getOrElse("")
    val password = config("password").stringOption.getOrElse("")
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
      throw new Throwable("Failed to connect")
    }

    ftp.login(username,password)

    if (!FTPReply.isPositiveCompletion(ftp.getReplyCode)) {
      ftp.disconnect()
      throw new Throwable("Failed to login")
    }

    ftp.enterLocalPassiveMode()

    if (!FTPReply.isPositiveCompletion(ftp.getReplyCode)) {
      ftp.disconnect()
      throw new Throwable("FTP server failed to go into passive mode")
    }

    ftp
  }

  def getFiles(config: DataSet, query: DataSet): List[String] = List[String]()
}
