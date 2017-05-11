package Term.Legacy

import Common.Data.{DataNothing, DataString}
import Common.DataSet

case class DataSetHttpResponse(label: String, uri: String, statusCode: Int, headers: Map[String, String], body: DataSet) extends DataSet {

  override def apply(field: String): DataSet =
    if (field == body.label) {
      body
    }
    else if (field == "status") {
      DataString(field, statusCode.toString)
    }
    else {
      headers.get(field).map(DataString(field, _)).getOrElse(DataNothing())
    }

  override def apply(num: Int): DataSet = body

  override def elems: Seq[DataSet] = List(body)

  override def toOption: Option[DataSet] = Some(this)
}
