package actio.datapipes.dataSources
import actio.common.Data.{DataNothing, DataSet}
import com.typesafe.scalalogging.Logger
import org.apache.commons.net.ftp._

class FTPDataSource extends FileDataSource {
  private val logger = Logger("FTPDataSource")

  var ftpconn: FTPClient = _

  override def init(config: DataSet): Unit = {
    ftpconn = connect(config)
  }

  override def readAndSendFiles(config: DataSet, filePath: String): Iterable[DataSet] = {

    val fs = ftpconn.retrieveFileStream(filePath)

    ftpconn.completePendingCommand()

    ftpconn.disconnect()

    List(DataNothing())
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

}
