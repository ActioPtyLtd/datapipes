package actio.datapipes.pipeline

import java.util.UUID

import actio.common.Data.{DataNothing, DataSet}
import actio.common.{Dom, Event, Observable, Observer}
import actio.datapipes.pipescript.Pipeline.Operation
import actio.datapipes.task.Task
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer

object SimpleExecutor {

  trait TaskOperation extends Observable[Dom] with Observer[Dom] {

    def start(): Unit = next(Dom())

    def start(ds: DataSet) = {
      val pipelineRunId = UUID.randomUUID().toString()
      val startEvent = Event(pipelineRunId, "","INFO", "START", "Started DataPipes Runtime")
      next(Dom() ~ Dom("start", Nil, ds, DataNothing(), startEvent :: Nil))
    }
  }

  val logger = Logger("SimpleExecutor")

  def getRunnable(operation: Operation): TaskOperation = operation match {

    case actio.datapipes.pipescript.Pipeline.Select(left, select, _) => new TaskOperation {

      val t = getRunnable(left)

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

        // add an event
        //val pipelineRunId = value.children.last.events.headOption.map(_.pipeInstanceId).getOrElse("")

        //val domWithEvent = Dom(value.label, value.children, value.success, value.error,
        //  Event(pipelineRunId,t.name,"INFO", "PROGRESS", s"Task ${t.name} received data", System.currentTimeMillis(), "", "size", size)
        //  :: value.events)

        //myTask.next(domWithEvent)
        myTask.next(value)
      }

      def completed(): Unit = { logger.info(s"=== Operation ${operation.name} completed ==="); myTask.completed() }

      def error(exception: Throwable): Unit = myTask.error(exception)

      def subscribe(observer: Observer[Dom]): Unit = myTask.subscribe(observer)
    }

    case p: actio.datapipes.pipescript.Pipeline.Pipe => new TaskOperation {

      val l = getRunnable(p.left)
      val r = getRunnable(p.right)

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
    val runnable = getRunnable(operation)

    runnable.subscribe(observer)
    runnable
  }
}
