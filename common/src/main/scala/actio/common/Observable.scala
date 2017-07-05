package actio.common

trait Observable[+T] {
  def subscribe(observer: Observer[T]): Unit
}
