package actio.datapipes.application

import java.io.InputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import actio.common.Data.{DataNothing, DataSet, DataString, JsonXmlDataSet}
import actio.common.{Dom, Observer}
import actio.datapipes.pipescript.Pipeline.PipeScript
import actio.datapipes.pipeline.SimpleExecutor.TaskOperation
import actio.datapipes.pipeline._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.json4s.{DefaultFormats, JValue, native}
import Directives._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport._
import akka.http.scaladsl.{ ConnectionContext, HttpsConnectionContext }

class AppService(pipeScript: PipeScript) {
  val logger = Logger("AppService")

  implicit val system = ActorSystem("datapipes-server")
  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  val taskListen = new TaskOperation {

    var response: Dom = Dom()

    def next(value: Dom): Unit = response = value

    def completed(): Unit = {}

    def error(exception: Throwable): Unit = ???

    def subscribe(observer: Observer[Dom]): Unit = ???
  }

  val pipeLine = (name: String) => pipeScript.pipelines.find(p => p.name == name).map(p => SimpleExecutor.getService(p, taskListen))

  val route: Route =
    path("datapipes" / ".*".r) { name =>
      (post & extract(_.request.entity.contentType.mediaType)) { ctype =>

        implicit val serialization = native.Serialization
        implicit val formats = DefaultFormats

        if (ctype == `application/json`)
          entity(as[JValue]) { requestJson =>
            val ds = JsonXmlDataSet.json2dsHelper(requestJson)
            handle(ds, name)
          }
        else
          reject
      } ~
      post {
        entity(as[String]) { str =>
          logger.info(s"POST received, using text body.")
          handle(DataString(str), name)
        }
      }
    }


  val port = pipeScript.settings("port").intOption.getOrElse(8080)
  val https = pipeScript.settings("ssl").toOption.map(ssl =>
    httpsContext(ssl("key-store-password").stringOption.get,
      ssl("key-store").stringOption.get,
      ssl("key-store-type").stringOption.get
    ))

  if(https.isDefined) {
    Http().setDefaultServerHttpContext(https.get)
    Http().bindAndHandle(route, "0.0.0.0", sys.props.get("http.port").fold(port)(_.toInt), https.get)
  }
  else
    Http().bindAndHandle(route, "0.0.0.0", sys.props.get("http.port").fold(port)(_.toInt))


  def handle(ds: DataSet, name: String) = {
    val call = pipeLine(name)
      if(call.isEmpty)
        reject
      else {
        call.get.start(ds)

        import JsonXmlDataSet.Extend

        taskListen.response.headOption.map(_.success) match {
          case Some(ds) =>
            if(ds == DataNothing())
              complete(StatusCodes.OK)
            else {
              ds match {
                case DataString(_, str) =>
                  complete(ds("status").intOption.getOrElse(200), str)
                case _ => {
                  implicit val serialization = native.Serialization
                  implicit val formats = DefaultFormats

                  complete(ds("status").intOption.getOrElse(200), ds.toJsonAST)
                }
              }
            }
          case _ => complete(StatusCodes.InternalServerError, "")
        }
      }
  }

  def httpsContext(pword: String, keyStoreName: String, keyStoreType: String): HttpsConnectionContext = {

    val password: Array[Char] = pword.toCharArray

    val ks: KeyStore = KeyStore.getInstance(keyStoreType)
    val keystore: InputStream = getClass.getClassLoader.getResourceAsStream(keyStoreName)

    require(keystore != null, "Keystore required!")
    ks.load(keystore, password)

    val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(ks, password)

    val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    tmf.init(ks)

    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)

    ConnectionContext.https(sslContext)
  }

}