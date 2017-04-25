package Common

trait Observer[T] {
  def completed()
  def error(exception: Exception);
  def next(value: T);
}
