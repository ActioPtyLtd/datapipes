package DataPipes.Common

trait Observable[+T] {
  def subscribe(observer: Observer[T]): Unit
}
