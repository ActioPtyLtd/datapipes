import DataPipes.Common.Data.{DataRecord, DataSet, DataString, JsonXmlDataSet}
import DataPipes.Common.{Dom, Observer}
import Pipeline.{Operation}
import SimpleExecutor.TaskOperation
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import org.json4s.{DefaultFormats, JValue, native}
import akka.http.scaladsl.model.MediaTypes._
import akka.stream.scaladsl.Sink

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class AppService(operation: Operation) {

  implicit val system = ActorSystem("datapipes-server")
  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  import Directives._
  import de.heikoseeberger.akkahttpjson4s.Json4sSupport._

  implicit val serialization = native.Serialization
  implicit val formats = DefaultFormats

  val taskListen = new TaskOperation {

    var response: Dom = Dom()

    def next(value: Dom): Unit = response = value

    def completed(): Unit = {}

    def error(exception: Throwable): Unit = ???

    def subscribe(observer: Observer[Dom]): Unit = ???
  }

  val pipeLine = SimpleExecutor.getService(operation, taskListen)

  val route: Route =
    path("datapipes") {
      (post & extract(_.request.entity.contentType.mediaType)) { ctype =>

        if (ctype == `application/json`)
          entity(as[JValue]) { requestJson =>
            val ds = JsonXmlDataSet.json2dsHelper("", requestJson)
            handle(ds)
          }
        else
          reject
      } ~
      (post & extract(_.request)) { req =>
        val str = Await.result(req.entity.dataBytes.map(b =>
          b.utf8String).runWith(Sink.lastOption), Duration(10, "seconds"))
        handle(DataString(str.getOrElse("")))
      }
    }

  def handle(ds: DataSet) = {
    pipeLine.start(ds)

    import JsonXmlDataSet.Extend

    taskListen.response.headOption.map(_.success) match {
      case Some(dr @ DataRecord(_, _)) => complete(200, dr.toJsonAST)
      case _ => complete(500, "")
    }
  }

  Http().bindAndHandle(route, "0.0.0.0", sys.props.get("http.port").fold(8080)(_.toInt))

}