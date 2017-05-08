package Task


import Common.Data.DataNothing
import Common._
import Term.TermExecutor

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.meta.Term

class TaskTerm(val name: String, val term: Term) extends Task {

  var _observer: Option[Observer[Dom]] = None

  def completed(): Future[Unit] = async {
    if(_observer.isDefined)
      await { _observer.get.completed() }
  }

  def error(exception: Throwable): Future[Unit] = ???

  def next(value: Dom): Future[Unit] = async {

    val nds = TermExecutor.eval(value, term)

    if(_observer.isDefined)
      await { _observer.get.next(value ~ Dom(name,null,List(),nds,DataNothing())) }
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer = Some(observer)
  }



}
