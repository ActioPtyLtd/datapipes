import Common.{Dom, Observable, Observer}
import Pipeline.Operation
import Task.Task

import scala.concurrent.Future
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global

object SimpleExecutor {

  trait TaskOperation extends Observable[Dom] with Observer[Dom] {

    def start(): Future[Unit] = next(Dom())
  }

  def getRunnable(operation: Operation): TaskOperation = operation match {

    case t: Pipeline.Task => new TaskOperation {

      val myTask = Task(t.name, t.taskType, t.config)

      def next(value: Dom): Future[Unit] = { println(s"=== Task ${t.name} received Dom ==="); myTask.next(value) }

      def completed(): Future[Unit] = myTask.completed()

      def error(exception: Throwable): Future[Unit] = myTask.error(exception)

      def subscribe(observer: Observer[Dom]): Unit = myTask.subscribe(observer)
    }

    case p: Pipeline.Pipe => new TaskOperation {

      val l = getRunnable(p.left)
      val r = getRunnable(p.right)

      l.subscribe(r)

      def next(value: Dom): Future[Unit] = { println(s"=== Pipe ${p.name} received Dom ==="); l.next(value) }

      def completed(): Future[Unit] = async {
        await { l.completed() }
        await { r.completed() }
      }

      def error(exception: Throwable): Future[Unit] = async {
        await { l.error(exception) }
        await { r.error(exception) }
      }

      def subscribe(observer: Observer[Dom]): Unit = r.subscribe(observer)
    }

  }
}
