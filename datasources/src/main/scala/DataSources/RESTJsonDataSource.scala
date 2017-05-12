package DataSources

import java.net.URI
import java.nio.charset.Charset

import DataPipes.Common.Data._
import DataPipes.Common.{DataSource, Observer, Parameters}
import com.typesafe.scalalogging.Logger

import scala.util.Try
import org.apache.commons.codec.binary.Base64
import org.apache.http._
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils
import JsonXmlDataSet._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}

import scala.concurrent.Future

class RESTJsonDataSource extends DataSource {

  val logger = Logger("RESTJsonDataSource")
  var _observer: Option[Observer[DataSet]] = None

  def subscribe(observer: Observer[DataSet]): Unit = _observer = Some(observer)



  private val CONTENT_TYPE: String = "application/json"

  var user: String = null
  var password: String = null

  type HttpClient = HttpUriRequest => (StatusLine, Array[Header], String)



  def createHttpRequest(label: String): HttpRequestBase = {
    if (label == "create" || label == "post") {
      new HttpPost()
    } else if (label == "update" || label == "put") {
      new HttpPut()
    } else if (label == "patch") {
      new HttpPatch()
    } else if (label == "delete") {
      new HttpDelete()
    } else {
      new HttpGet()
    }
  }

  def executeQueryLabel(ds: DataSet, label: String): DataSet = {

    val uri = ds("query")(label)("uri").stringOption

    if(uri.isDefined) {

      val userOption = ds("credential")("username").stringOption
      val passwordOption = ds("credential")("password").stringOption

      val authHeader = for {
        u <- userOption
        p <- passwordOption } yield new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + new String(Base64.encodeBase64((u + ":" + p).getBytes(Charset.forName("ISO-8859-1")))))

      val otherHeaders = ds("headers").elems.map(h => new BasicHeader(h.label, h.stringOption.getOrElse(""))).toList

      val headers: Seq[Header] = authHeader.map(a => a :: otherHeaders).getOrElse(otherHeaders)

      val requestQuery =
        createRequest(ds("query")(label)("body"),
          createHttpRequest(ds("query")(label)("verb").stringOption.getOrElse("get")),
          uri.get, headers)

      logger.info(s"Calling ${requestQuery.getMethod} ${requestQuery.getURI}")

      val element = getResponseDataSet(requestQuery)(sendRequest)

      element
    }
    else
      DataNothing()
  }

  private def createRequest(body: DataSet, verb: => HttpRequestBase, uri: String, headers: Seq[Header]): HttpRequestBase =
    verb match {
      case postput: HttpEntityEnclosingRequestBase => createRequest(body match {
        case DataString(_, s) => Some(s)
        case _ => Some(body.toJson)
      }, verb, uri, headers)
      case _ => createRequest(None, verb, uri, headers)
    }

  private def createRequest(body: Option[String], verb: => HttpRequestBase, uri: String, headers: Seq[Header]): HttpRequestBase = {
    val request = verb
    request.setURI(URI.create(uri))
    headers.foreach(h => request.setHeader(h.getName, h.getValue))

    if (body.isDefined) {

      logger.info(body.get)

      val input: StringEntity = new StringEntity(body.get,"UTF-8")
      input.setContentType(CONTENT_TYPE)
      request.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(input)
    }

    request
  }

  def getResponseDataSet(request: HttpUriRequest)(implicit httpClient: HttpClient): DataSet = {
    val response = httpClient(request)

    val displayString:String = Option(response._3).getOrElse("")
    if (displayString.length > 0) {
      logger.info(s"Body: '" + displayString.substring(0, Math.min(displayString.length, 500)) + "'")
    }

    val dsBody = Try(JsonXmlDataSet.fromJson(response._3)).toOption.getOrElse(DataString(Option(response._3).getOrElse("")))

    DataRecord("response",
      DataString("uri", request.getURI.toString) ::
      DataNumeric("status", response._1.getStatusCode) ::
        DataRecord("root", dsBody.elems.toList) ::
        response._2.map(h => DataString(h.getName,h.getValue)).toList
      )
  }

  def sendRequest(request: HttpUriRequest): (StatusLine, Array[Header], String) = {

    headers.foreach(t => request.setHeader(new BasicHeader(t._1, t._2.replace("\"", ""))))

    logger.info(">>>>>>>>" + request.toString + "<<<<<<<" + request.getRequestLine)

    request.getAllHeaders.foreach(f => logger.info(">>>" + f.getName + ">>" + f.getValue))

    val httpreq = HttpClientBuilder.create()

    val builthttp = httpreq.build()

    val response = builthttp.execute(request)
    val respEntity = response.getEntity

    val ret = (response.getStatusLine,
      response.getAllHeaders,
      if (Option(respEntity).isDefined) EntityUtils.toString(response.getEntity, "UTF-8") else "")

    response.close()
    ret
  }

  var headers: List[(String, String)] = List()

  override def exec(parameters: Parameters): Future[Unit] = async {
    val ds = executeQueryLabel(parameters, parameters("label").stringOption.getOrElse(parameters("query").headOption.map(_.label).get))

    if(_observer.isDefined)
      await {
        _observer.get.next(ds)
      }
  }
}
