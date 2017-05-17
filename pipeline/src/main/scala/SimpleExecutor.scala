import DataPipes.Common.Data.{DataNothing, DataSet}
import DataPipes.Common._
import Pipeline.Operation
import _root_.Task._
import com.typesafe.scalalogging.Logger

object SimpleExecutor {

  trait TaskOperation extends Observable[Dom] with Observer[Dom] {

    def start(): Unit = next(Dom())

    def start(ds: DataSet) =  next(Dom() ~ Dom("start", null, Nil, ds, DataNothing()))
  }

  val logger = Logger("SimpleExecutor")

  def getRunnable(operation: Operation): TaskOperation = operation match {

    case t: Pipeline.Task => new TaskOperation {

      val myTask = Task(t.name, t.taskType, t.config)

      def next(value: Dom): Unit = { logger.debug(s"=== Task ${t.name} received Dom ==="); myTask.next(value) }

      def completed(): Unit = { logger.info(s"=== Operation ${operation.name} completed ==="); myTask.completed() }

      def error(exception: Throwable): Unit = myTask.error(exception)

      def subscribe(observer: Observer[Dom]): Unit = myTask.subscribe(observer)
    }

    case p: Pipeline.Pipe => new TaskOperation {

      val l = getRunnable(p.left)
      val r = getRunnable(p.right)

      l.subscribe(r)

      def next(value: Dom): Unit = {
        logger.debug(s"=== Pipe ${p.name} received Dom ===")
        l.next(value)
      }

      def completed(): Unit = {
        logger.info(s"=== Operation ${operation.name} completed ===")
        l.completed()
        r.completed()
      }

      def error(exception: Throwable): Unit = {
        l.error(exception)
        r.error(exception)
      }

      def subscribe(observer: Observer[Dom]): Unit = r.subscribe(observer)
    }

  }
}
