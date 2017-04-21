package Common


trait SimpleIterator[T] {
  def headOption(): Option[T]
}