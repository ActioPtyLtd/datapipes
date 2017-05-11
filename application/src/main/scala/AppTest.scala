
import scala.concurrent.Await
import scala.concurrent.duration._
import Pipeline._

object AppTest extends App {

  val pf = Builder.build(ConfigReader.read("/home/maurice/bitbucket/datapipes/test.conf"))

  println(pf)

  val run = SimpleExecutor.getRunnable(pf.get.pipeline).start()

  Await.result(run, 1000000 millis)

}
