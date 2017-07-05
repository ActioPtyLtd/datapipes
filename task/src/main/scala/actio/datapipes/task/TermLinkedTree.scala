package actio.datapipes.task

trait TermLinkedTree {
  def label: String
}

case class TermNode(label: String, fields: List[TermLinkedTree]) extends TermLinkedTree {

}

case class TermLeaf(label: String, term: scala.meta.Term) extends TermLinkedTree {

}