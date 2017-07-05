package actio.datapipes.application

import DataPipes.Common.Data._
import DataPipes.Common.{Dom, Observer}
import actio.datapipes.pipescript.Pipeline.PipeScript
import actio.datapipes.pipeline.SimpleExecutor.TaskOperation
import actio.datapipes.pipeline._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.json4s.{DefaultFormats, JValue, native}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class AppService(pipeScript: PipeScript) {

  implicit val system = ActorSystem("datapipes-server")
  implicit val materializer = ActorMaterializer()

  import Directives._
  import de.heikoseeberger.akkahttpjson4s.Json4sSupport._
  import system.dispatcher

  implicit val serialization = native.Serialization
  implicit val formats = DefaultFormats

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

        if (ctype == `application/json`)
          entity(as[JValue]) { requestJson =>
            val ds = JsonXmlDataSet.json2dsHelper("", requestJson)
            handle(ds, name)
          }
        else
          reject
      } ~
      (post & extract(_.request)) { req =>
        val str = Await.result(req.entity.dataBytes.map(b =>
          b.utf8String).runWith(Sink.lastOption), Duration(10, "seconds"))
        handle(DataString(str.getOrElse("")), name)
      }
    }

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
            else
              complete(ds("status").intOption.getOrElse(200), ds.toJsonAST)
          case _ => complete(StatusCodes.InternalServerError, "")
        }
      }
  }


  val port = pipeScript.settings("port").intOption.getOrElse(8080)
  Http().bindAndHandle(route, "0.0.0.0", sys.props.get("http.port").fold(port)(_.toInt))

}