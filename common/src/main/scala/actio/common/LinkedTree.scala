package actio.common

abstract class LinkedTree[D <: LinkedTree[D]] {

  def apply(ord: Int): D

  def apply(field: String): D

  def toOption: Option[D]

  def elems: Seq[D]

  def label: String

  def stringOption: Option[String] = None

  def intOption: Option[Int] = None

  def headOption: Option[D] = elems.headOption

  def map[E](f:D => E) = elems.map(f)

  def flatMap[E](f:D => Seq[E]) = elems.flatMap(f)
}
