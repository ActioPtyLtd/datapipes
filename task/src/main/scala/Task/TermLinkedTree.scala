package Task

import scala.meta.Term

trait TermLinkedTree {
  def label: String
}

case class TermNode(label: String, fields: List[TermLinkedTree]) extends TermLinkedTree {

}

case class TermLeaf(label: String, term: Term) extends TermLinkedTree {

}