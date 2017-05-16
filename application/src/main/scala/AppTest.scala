
import scala.concurrent.Await
import scala.concurrent.duration._
import Pipeline._

object AppTest extends App {

  val pf = Builder.build(ConfigReader.read("/home/maurice/bitbucket/datapipes/test.conf"))

  println(pf)

  SimpleExecutor.getRunnable(pf.get.pipeline).start()

}
