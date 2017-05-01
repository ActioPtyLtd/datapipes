package Common

trait DomTrait extends LinkedTree[DomTrait] {
  def success: DataSet
  def error: DataSet
}

case class Dom(label: String, parent: Dom, children: List[Dom], success: DataSet, error: DataSet) extends DomTrait {

  def toOption: Option[DomTrait] = None

  def elems: Seq[DomTrait] = children

  def apply(field: String): DomTrait = ???

  def apply(num: Int): DomTrait = ???
}

