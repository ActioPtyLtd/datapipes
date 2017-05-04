
import scala.concurrent.Await
import scala.concurrent.duration._
import Common.Data.{DataNothing, DataRecord, DataString}
import Pipeline._

object AppTest extends App {

  val p = Pipe("My Pipe Right", Pipe("My Pipe Left",
    Task("My Extract Task", "TaskExtract", DataNothing()),
    Task("My Term Task", "TaskTerm", DataRecord(DataString("term", "ds => ds + \" \" + substring(toUpperCase(ds),\"1\")")))),
    Task("My Print", "TaskPrint", DataNothing()))

  val run = SimpleExecutor.getRunnable(p).start()

  Await.result(run, 100000 millis)

}
