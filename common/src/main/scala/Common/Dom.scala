package Common

import Common.Data.{DataNothing, DataRecord}

trait DomTrait extends LinkedTree[DomTrait] {
  def success: DataSet
  def error: DataSet

  def headOption: Option[Dom]
}

case class Dom(label: String, parent: Dom, children: List[Dom], success: DataSet, error: DataSet) extends DomTrait {

  lazy val mapFields: Map[String, Dom] = children.map(f => f.label -> f).toMap

  def toOption: Option[DomTrait] = None

  def elems: Seq[DomTrait] = children

  def apply(field: String): DomTrait = mapFields(field)

  def apply(num: Int): DomTrait = ???

  def ~(other: Dom): Dom = {
    Dom(label, null, other :: this.children, success, error)
  }

  def headOption: Option[Dom] = children.headOption
}

object Dom {

  def apply() = new Dom("",null, List(), DataNothing(), DataNothing())

  import scala.language.implicitConversions

  implicit def dom2DataSet(dom: Dom): DataSet = DataRecord(dom.label, dom.success :: dom.children.map(d => dom2DataSet(d)))
}