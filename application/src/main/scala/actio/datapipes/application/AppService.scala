package actio.datapipes.application

import java.io.InputStream
import java.net.URI
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import actio.common.Data._
import actio.common.{Dom, Observer}
import actio.datapipes.pipescript.Pipeline.{PipeScript, Service}
import actio.datapipes.pipeline.SimpleExecutor.TaskOperation
import actio.datapipes.pipeline._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.{HttpHeader, HttpRequest, StatusCodes}
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.json4s.{DefaultFormats, JValue, native}
import Directives._
import akka.http.scaladsl.model.headers.RawHeader
import de.heikoseeberger.akkahttpjson4s.Json4sSupport._
import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import akka.stream.scaladsl.{Sink, Source}

import scala.meta.Term

class AppService(pipeScript: PipeScript) {
  val logger = Logger("AppService")

  implicit val system = ActorSystem("datapipes-server")
  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  val pipeLine = (name: String, task: TaskOperation) => pipeScript.pipelines.find(p => p.name == name).map(p => SimpleExecutor.getService(p.pipe, task))

  val route: Route = getRoute(pipeScript)
//    path("datapipes" / pipeScript.services.head.path.r) { name =>
//      (get & extract(_.request.headers)) { headers =>
//        logger.info(s"GET received.")
//        extract(_.request.uri.query().toSeq) { params =>
//          val dataSet = DataArray(DataRecord(DataRecord("headers", headers.map(h => DataString(h.name(), h.value())).toList),
//            DataRecord("params", params.map(p => DataString(p._1, p._2)).toList)
//          ))
//          handle(dataSet, pipeScript.services.head.get.get.name)
//        }
//      } ~
//      (post & extract(_.request.entity.contentType.mediaType)) { ctype =>
//
//        implicit val serialization = native.Serialization
//        implicit val formats = DefaultFormats
//
//        if (ctype == `application/json`)
//          entity(as[JValue]) { requestJson =>
//            val ds = JsonXmlDataSet.json2dsHelper(requestJson)
//            handle(ds, name)
//          }
//        else
//          reject
//      } ~
//        (post & extract(_.request.headers)) { headers =>
//          entity(as[String]) { str =>
//            logger.info(s"POST received, using text body.")
//            val dataSet = DataArray(DataRecord(DataString("body", str), DataRecord("headers", headers.map(h => DataString(h.name(), h.value())).toList)))
//            handle(dataSet, name)
//          }
//        }
//    }

  val port = pipeScript.settings("port").intOption.getOrElse(8080)
  val https = pipeScript.settings("ssl").toOption.map(ssl =>
    httpsContext(
      ssl("key-store-password").stringOption.get,
      ssl("key-store").stringOption.get,
      ssl("key-store-type").stringOption.get
    ))

  if (https.isDefined) {
    Http().setDefaultServerHttpContext(https.get)
    Http().bindAndHandle(route, "0.0.0.0", sys.props.get("http.port").fold(port)(_.toInt), https.get)
  } else
    Http().bindAndHandle(route, "0.0.0.0", sys.props.get("http.port").fold(port)(_.toInt))

  def handle(ds: DataSet, name: String) = {

    val taskListen = new TaskOperation {

      var response: Dom = Dom()

      def next(value: Dom): Unit = response = value

      def completed(): Unit = {}

      def error(exception: Throwable): Unit = ???

      def subscribe(observer: Observer[Dom]): Unit = ???

      val totalProcessed = 0
      val totalProcessedSize = 0
      val totalError = 0
      val totalErrorSize = 0
    }

    val call = pipeLine(name, taskListen)
    if (call.isEmpty)
      reject
    else {
      call.get.start(ds)

      import JsonXmlDataSet.Extend

      taskListen.response.success match {
        case DataNothing(_) =>
          complete(StatusCodes.OK)
        case DataString(_, str) =>
          complete(ds("status").intOption.getOrElse(200), str)
        case rts => {
          implicit val serialization = native.Serialization
          implicit val formats = DefaultFormats

          complete(ds("status").intOption.getOrElse(200), rts.toJsonAST)
        }
      }
    }
  }

  def getRoute(pipeScript: PipeScript): Route = {
    if (pipeScript.services.size == 1)
      getServiceRoute(pipeScript, pipeScript.services.head)
    else
      pipeScript.services.map(getServiceRoute(pipeScript,_)).reduceLeft((a, b) => a ~ b)
  }

  def getServiceRoute(pipeScript: PipeScript, service: Service): Route = {

    val urlPath = service.path.split("/").toList

    (path(Segments) | path(Segments /)) { segments =>
      logger.info(segments.size.toString)

      if(segments.size != urlPath.size ||
        (urlPath.last.endsWith("*") && segments.size > (urlPath.size))
      )
        reject
      else {

        val zs = segments zip urlPath

        if (zs.exists(z => z._2 != "*" && !z._2.startsWith("$") && z._2 != z._1))
          reject
        else if (service.proxy.isDefined) {
          (ctx: RequestContext) => {
            logger.info(ctx.request.method.toString())

            logger.info("Proxy to: " + service.proxy.get._1 + "/" + segments.drop(zs.size - 1).mkString("/") +
              (if(ctx.request.uri.toString.endsWith("/")) "/" else ""))
            ctx.complete(Http().singleRequest(ctx.request
              .withHeaders(ctx.request.headers :+
                              RawHeader("X-Forwarded-Host", ctx.request.getUri.getHost.toString + ":" + ctx.request.getUri.getPort + "/" +
                                zs.dropRight(1).map(_._1).mkString("/")) :+
                              RawHeader("X-Forwarded-Proto", ctx.request.uri.scheme)
                            )
              //  .withUri("http://localhost:49160/files/3dbe65d656433f60f9891a929c6b6f30"))
              .withUri(service.proxy.get._1 + "/" + segments.drop(zs.size - 1).mkString("/") +
              (if(ctx.request.uri.toString.endsWith("/")) "/" else "")))
            )
          }
        }
//        else if(service.proxy.isDefined) {
//          (ctx: RequestContext) =>
//            logger.info("Proxy to: " + service.proxy.get._1 + "/" + segments.drop(zs.size - 1).mkString("/") + (if(ctx.request.uri.toString.endsWith("/")) "/" else ""))
//            Source.single(ctx.request
//              .withHeaders(ctx.request.headers :+
//                                            RawHeader("X-Forwarded-Host", ctx.request.getUri.getHost.toString + ":" + ctx.request.getUri.getPort + "/" +
//                                              zs.dropRight(1).map(_._1).mkString("/")) :+
//                                            RawHeader("X-Forwarded-Proto", ctx.request.uri.scheme)
//                                          )
//              .withUri(service.proxy.get._1 + "/" + segments.drop(zs.size - 1).mkString("/") +
//                (if(ctx.request.uri.toString.endsWith("/")) "/" else ""))
//              )
//              .via(Http(system).outgoingConnection(new URI(service.proxy.get._1).getHost,service.proxy.get._2))
//              .runWith(Sink.head)
//              .flatMap(ctx.complete(_))
//
//        }
        else {
          extract(_.request.headers) { headers =>
            (get & extract(_.request.uri.query().toSeq)) { params =>
              if (service.get.isEmpty)
                reject
              else {
                val dataSet = DataArray(DataRecord(DataRecord("headers", headers.map(h => DataString(h.name(), h.value())).toList),
                  DataRecord("params", params.map(p => DataString(p._1, p._2)).toList),
                  DataRecord("variables", zs.filter(f => f._2.startsWith("$")).map(m => DataString(m._2.drop(1), m._1)))
                ))

                handle(dataSet, service.get.get.name)
              }
            } ~
            ((post | put) & extract(_.request.entity.contentType.mediaType)) { ctype =>
               {

                implicit val serialization = native.Serialization
                implicit val formats = DefaultFormats

                if (ctype == `application/json`)
                  entity(as[JValue]) { requestJson =>
                    val ds = DataArray(DataRecord(DataRecord("headers", headers.map(h => DataString(h.name(), h.value())).toList),
                      DataRecord("variables", zs.filter(f => f._2.startsWith("$")).map(m => DataString(m._2.drop(1), m._1))),
                      Operators.relabel(JsonXmlDataSet.json2dsHelper(requestJson),"body")
                    ))

                    post {
                      if (service.post.isEmpty)
                        reject
                      else
                        handle(ds, service.post.get.name)
                    } ~
                    put {
                      if (service.put.isEmpty)
                        reject
                      else
                        handle(ds, service.put.get.name)
                    }
                  }
                else
                  reject
              }
            }
          }
        }
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