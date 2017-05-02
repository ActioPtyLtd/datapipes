
import scala.concurrent.{Await}
import scala.concurrent.duration._
import Common.Data.{DataNothing}

import Common._
import Pipeline._

object AppTest extends App {

  val p = Pipe("My Pipe", Task("My Extract Task", "TaskExtract", DataNothing()), Task("My Print", "TaskPrint", DataNothing()))

  val runnable = SimpleExecutor.getRunnable(p)

  var f = runnable.next(Dom())

  Await.result(f, 100000 millis)

}
