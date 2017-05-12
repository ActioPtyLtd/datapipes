package DataSources

import java.net.URI
import java.nio.charset.Charset

import DataPipes.Common.Data._
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

class RESTJsonDataSource {

  val logger = Logger("RESTJsonDataSource")

  private val CONTENT_TYPE: String = "application/json"

  var user: String = null
  var password: String = null

  type HttpClient = HttpUriRequest => (StatusLine, Array[Header], String)

  def authHeader: String = "Basic " + new String(Base64.encodeBase64((user + ":" + password).getBytes(Charset.forName("ISO-8859-1"))))

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
    val requestQuery =
      createRequest(DataNothing(),
        createHttpRequest(""),
        "")

    logger.info(s"Calling ${requestQuery.getMethod} ${requestQuery.getURI}")

    val element = getResponseDataSet(requestQuery)(sendRequest)

    /*
    if(element.statusCode >= 400 && element.statusCode < 600) {

      logger.error(s"Status code ${element.statusCode} returned.")
      logger.error(s"Body "+element.body.toString)
      logger.error("{" + Data2Json.toJsonString(element.body) + "}")
      if (onError != null)
        if (onError.toLowerCase  == "exit"){
          // exiting pipeline

          logger.error(s"**** Exiting OnError ${element.statusCode} returned.")

          System.exit(-1)
        }   else   if (onError.toLowerCase == "exception"){
          // exiting pipeline

          logger.error(s"**** On Error Exception: DataSourceREST ${element.statusCode} returned.")

          throw new Exception("On Error Exception: DataSourceREST ")
        }
    } else {
      logger.info(s"Status code ${element.statusCode} returned.")
    }
*/
    element
  }

  private def createRequest(body: DataSet, verb: => HttpRequestBase, uri: String): HttpRequestBase =
    verb match {
      case postput: HttpEntityEnclosingRequestBase => createRequest(body match {
        case DataString(_, s) => Some(s)
        case _ => Some(body.toJson)
      }, verb, uri)
      case _ => createRequest(None, verb, uri)
    }

  private def createRequest(body: Option[String], verb: => HttpRequestBase, uri: String): HttpRequestBase = {
    val request = verb
    request.setURI(URI.create(uri))
    request.setHeader(HttpHeaders.AUTHORIZATION, authHeader)

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
        dsBody ::
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
}
