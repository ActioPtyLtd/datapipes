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
  }

  val logger = Logger("SimpleExecutor")

  def getRunnable(operation: Operation, eventOperation: Option[TaskOperation]): TaskOperation = operation match {

    case actio.datapipes.pipescript.Pipeline.Select(left, select, _) => new TaskOperation {

      val t = getRunnable(left, eventOperation)

      override def next(value: Dom): Unit = {
        logger.debug(s"=== Task ${left.name} to receive Dom $select ===")

        val sel = value.children.find(f => f.label == select).getOrElse(throw new Exception(s"Dom with name $select does not exist"))

        t.next(sel)
      }

      override def completed(): Unit = t.completed()

      override def error(exception: Throwable): Unit = t.error(exception)

      override def subscribe(observer: Observer[Dom]): Unit = t.subscribe(observer)
    }

    case t: actio.datapipes.pipescript.Pipeline.Task => new TaskOperation {

      val myTask = Task(t.name, t.taskType, t.config)

      def next(value: Dom): Unit = {
        val size = value.success.elems.size

        logger.debug(s"=== Task ${t.name} received Dom with last successful DataSet of size ($size) ===")

        // get the events coming into this task and send them to the events pipeline
        eventOperation.foreach{ o =>
          if(value.events.nonEmpty) {
            val eventDom = Dom() ~ Dom("start", Nil, DataArray(value.events.map(e => Event.toDataSet(e))), DataNothing(), Nil)
            o.next(eventDom)
          }
        }

        myTask.next(value)
      }

      def completed(): Unit = {

        logger.info(s"=== Operation ${operation.name} completed ===");

        eventOperation.foreach{ o =>
            val eventDom = Dom() ~ Dom("start", Nil, DataArray(Event.toDataSet(
              Event(pipelineRunId, operation.name, "INFO", "FINISH", "Task completed")
            )), DataNothing(), Nil)
            o.next(eventDom)
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

        override def next(value: Dom): Unit = _observer.foreach(o =>
          o.next(parentDom ~ value)
        )

        override def completed(): Unit = _observer.foreach(o => o.completed())

        override def error(exception: Throwable): Unit = _observer.foreach(o => o.error(exception))

        override def subscribe(observer: Observer[Dom]): Unit = _observer.append(observer)
      }

      l.subscribe(i)
      i.subscribe(r)

      def next(value: Dom): Unit = {
        i.setNewDom(value)
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

  def getService(operation: Operation, observer: Observer[Dom]): TaskOperation = {
    val runnable = getRunnable(operation, None)

    runnable.subscribe(observer)
    runnable
  }
}
