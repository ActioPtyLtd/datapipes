package DataPipes.Common.Data

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.Xml.{toJson, toXml}

import scala.xml.{Attribute, _}

object PrettyPrint {

  implicit class PrettyPrint(data: DataSet) {
    def print(): String = data match {
      case DataString(l, s) => l + " -> \"" + s + "\""

      case DataRecord(key, fs) =>
        "(" + key + "," +
          fs.map(f => f.print()).mkString(",") +
          ")"
      case DataArray(key, fs) =>
        "[" + key + "," +
          fs.map(f => f.print()).mkString(",") +
          "]"
      case DataNothing(_) => "()"
      case DataNumeric(l, num) => l + " -> " + num.setScale(2, BigDecimal.RoundingMode.HALF_UP).underlying().stripTrailingZeros().toPlainString
      case DataBoolean(l, bool) => l + " -> " + bool.toString
      case DataDate(l, date) => l + " -> " + date.toString
    }

    def toJsonAST: JValue = data match {
      case DataString(_, s) => JString(s)
      case DataRecord(_, fs) => JObject(fs.map(f => (f.label, f.toJsonAST)))
      case DataNumeric(_, num) => JDouble(num.toDouble)
      case DataBoolean(_, bool) => JBool(bool)
      case DataDate(_, date) => JString(date.toString)
      case DataArray(_, fs) => JArray(fs.map(f => f.toJsonAST))
      case DataNothing(_) => JNull
    }

    def toJson: String = pretty(render(data.toJsonAST))

    def toXmlAST: Node = data match {
      case DataString(l, s) => Elem(null, l, Null, TopScope, false, Text(s))
      case r @ DataRecord(l, fs) if fs.map(_.label).contains("attributes") =>
        Elem(null, l, r("attributes").elems.collect { case ds: DataString => ds }.toList.foldLeft(Null: MetaData)((a: MetaData,b: DataString) =>  a.append(Attribute(None, b.label, Text(b.str), Null))), TopScope, false, fs.filterNot(w => w.label == "attributes")map(f => f.toXmlAST) :_*)
      case DataRecord(l, fs) => Elem(null, l, Null, TopScope, false, fs.map(f => f.toXmlAST) :_*)
      case DataArray(l, fs) => Elem(null, l, Null, TopScope, false, fs.map(f => f.toXmlAST) :_*)
      case DataNumeric(l, num) => Elem(null, l, Null, TopScope, false, Text(num.toDouble.toString))
      case DataBoolean(l, bool) => Elem(null, l, Null, TopScope, false, Text(bool.toString))
      case DataDate(l, date) => Elem(null, l, Null, TopScope, false, Text(date.toString))
      case DataNothing(l) => Elem(null, l, Null, TopScope, false, Text(""))
    }

    def toXml: String = toXmlAST.toString()
  }

}