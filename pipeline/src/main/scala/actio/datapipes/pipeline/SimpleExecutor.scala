package actio.datapipes.pipeline

import java.util.UUID

import actio.common.Data.{DataArray, DataNothing, DataSet}
import actio.common.{Dom, Event, Observable, Observer}
import actio.datapipes.pipescript.Pipeline.Operation
import actio.datapipes.task.Task
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer

object SimpleExecutor {

  // global for now
  val pipelineRunId = UUID.randomUUID().toString

  trait TaskOperation extends Observable[Dom] with Observer[Dom] {

    def start(): Unit = next(Dom())

    def start(ds: DataSet) = {
      next(Dom() ~ Dom("start", Nil, ds, DataNothing(), Nil))
    }

    def totalProcessed: Int
    def totalProcessedSize: Int
    def totalError: Int
    def totalErrorSize: Int
  }

  val logger = Logger("SimpleExecutor")

  def getRunnable(operation: Operation, eventOperation: Option[(List[Event]) => Unit]): TaskOperation = operation match {

    case actio.datapipes.pipescript.Pipeline.Select(left, select, _) => new TaskOperation {

      val t = getRunnable(left, eventOperation)

      override def next(value: Dom): Unit = {
        logger.debug(s"=== Task ${left.name} to receive Dom $select ===")

        val sel = value.children.find(f => f.label == select).getOrElse(throw new Exception(s"Dom with name $select does not exist"))

        t.next(sel)
        _totalError = _totalError + t.totalError

      }

      override def completed(): Unit = {
        t.completed()
      }

      override def error(exception: Throwable): Unit = t.error(exception)

      override def subscribe(observer: Observer[Dom]): Unit = t.subscribe(observer)

      var _totalError = 0

      val totalProcessed = 0
      val totalProcessedSize = 0
      def totalError = _totalError
      val totalErrorSize = 0
    }

    case t: actio.datapipes.pipescript.Pipeline.Task => new TaskOperation {

      val myTask = Task(t.name, t.taskType, t.config)

      var totalProcessed = 0
      var totalProcessedSize = 0
      var totalError = 0
      var totalErrorSize = 0

      def next(value: Dom): Unit = {
        val size = value.success.elems.size

        try {

          logger.debug(s"=== Task ${t.name} received Dom with last successful DataSet of size ($size) ===")

          myTask.next(value)

          totalProcessed = totalProcessed + 1
          totalProcessedSize = totalProcessedSize + size
        }
        catch {
          case e: Exception => {
            logger.error(s"=== Task ${t.name} failed to process last successful DataSet of size ($size) ===")
            logger.error(e.getMessage)

            eventOperation.foreach{ eo =>
              eo(List(Event.taskError(pipelineRunId, operation.name, 1, size,
                0, 0, e)))
            }

            totalError = totalError + 1
            totalErrorSize = totalErrorSize + size
          }
        }
      }

      def completed(): Unit = {

        logger.info(s"=== Operation ${operation.name} completed ===");

        eventOperation.foreach{ eo =>
          eo(List(Event.taskCompleted(pipelineRunId, operation.name, totalError, totalErrorSize,
            totalProcessed, totalProcessedSize, s"Task ${operation.name} completed.")))
        }

        myTask.completed()
      }

      def error(exception: Throwable): Unit = myTask.error(exception)

      def subscribe(observer: Observer[Dom]): Unit = myTask.subscribe(observer)
    }

    case p: actio.datapipes.pipescript.Pipeline.Pipe => new TaskOperation {

      val l = getRunnable(p.left, eventOperation)
      val r = getRunnable(p.right, eventOperation)

      // could alternatively, un-subscribe and subscribe new task, rather than mutate dom here
      val i = new TaskOperation {
        val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
        var parentDom: Dom = _

        def setNewDom(newDom: Dom) = parentDom = newDom

        override def next(value: Dom): Unit = _observer.foreach { o =>
          try {
            if(value.events.nonEmpty) {
              eventOperation.foreach(eo => eo(value.events))
            }

            o.next(parentDom ~ value)
          } catch {
            case e: Exception => {
              _totalError = _totalError + 1
            }
          }
        }

        override def completed(): Unit = _observer.foreach(o => o.completed())

        override def error(exception: Throwable): Unit = _observer.foreach(o => o.error(exception))

        override def subscribe(observer: Observer[Dom]): Unit = _observer.append(observer)

        var _totalError = 0

        val totalProcessed = 0
        val totalProcessedSize = 0
        def totalError =  _totalError
        val totalErrorSize = 0
      }

      l.subscribe(i)
      i.subscribe(r)

      def next(value: Dom): Unit = {
        i.setNewDom(value)
        l.next(value)
        _totalError = _totalError + l.totalError
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

      var _totalError = 0

      val totalProcessed = 0
      val totalProcessedSize = 0
      def totalError = _totalError
      val totalErrorSize = 0
    }

    case t: actio.datapipes.pipescript.Pipeline.Sequence => new TaskOperation {

      val l = getRunnable(t.left, eventOperation)
      val r = getRunnable(t.right, eventOperation)

      val totalProcessed: Int = 0
      val totalProcessedSize: Int = 0
      val totalError: Int = 0
      val totalErrorSize: Int = 0

      override def completed(): Unit = {
        l.completed()
        r.completed()
      }

      override def error(exception: Throwable): Unit = {
        l.error(exception)
        r.error(exception)
      }

      override def next(value: Dom): Unit = {
          logger.info(s"Commencing operation ${t.left.name}...")
          l.next(value) // not checking if any errors were raised

          t match {
            case actio.datapipes.pipescript.Pipeline.SequenceOnSuccess(_,_,_) if l.totalError>0  => {
              logger.info(s"Operation ${t.left.name} has reported an error. Operation ${t.right.name} will not be executed.")
            }
            case _ => {
              logger.info(s"Operation ${t.left.name} has completed. Commencing operation ${t.right.name}...")
              r.next(value)
              logger.info(s"Operation ${t.right.name} has completed.")
            }
          }
      }

      override def subscribe(observer: Observer[Dom]): Unit = {

      }
    }
  }

  def getService(operation: Operation, observer: Observer[Dom]): TaskOperation = {
    val runnable = getRunnable(operation, None)

    runnable.subscribe(observer)
    runnable
  }
}
