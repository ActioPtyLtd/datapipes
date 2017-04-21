package Common

/**
  * Created by maurice on 21/04/17.
  */
trait Data extends LinkedTree[Data] {

}

abstract class LinkedTree[D <: LinkedTree[D]] {

  def apply(ord: Int): D

  def apply(field: String): D

  def toOption: Option[D]

  def elems: Seq[D]

  def label: String

  def stringOption: Option[String] = None
}