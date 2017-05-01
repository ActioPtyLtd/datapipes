package Common.Data

/**
  * Created by maurice on 1/05/17.
  */
abstract class LinkedTree[D <: LinkedTree[D]] {

  def apply(ord: Int): D

  def apply(field: String): D

  def toOption: Option[D]

  def elems: Seq[D]

  def label: String

  def stringOption: Option[String] = None
}
