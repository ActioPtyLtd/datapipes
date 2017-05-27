package DataPipes.Common

import DataPipes.Common.Data.{DataNothing, DataRecord, DataSet}

trait DomTrait extends LinkedTree[DomTrait] {
  def success: DataSet

  def error: DataSet

}

case class Dom(label: String, children: List[Dom], success: DataSet, error: DataSet) extends DomTrait {

  lazy val mapFields: Map[String, Dom] = children.map(f => f.label -> f).toMap

  def toOption: Option[DomTrait] = None

  def elems: Seq[DomTrait] = children

  def apply(field: String): DomTrait = mapFields(field)

  def apply(num: Int): DomTrait = ???

  def ~(other: Dom): Dom = {
    Dom(label, other :: this.children, success, error)
  }

}

object Dom {

  def apply() = new Dom("", List(), DataNothing(), DataNothing())

  import scala.language.implicitConversions

  implicit def dom2DataSet(dom: Dom): DataSet = DataRecord(dom.label, dom.success :: dom.children.map(d => dom2DataSet(d)))
}