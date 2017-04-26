package Common

import scala.concurrent.Future

trait Observer[-T] {
  def completed(): Future[Unit]
  def error(exception: Throwable): Future[Unit]
  def next(value: T): Future[Unit]
}
