
import scala.concurrent.Await
import scala.concurrent.duration._
import Pipeline._

object AppTest extends App {

  val config = ConfigReader.read("/home/maurice/bitbucket/datapipes/test/gnm_sunix_lens_export.conf")
  val pf = Builder.build(config)

  println(pf)

  SimpleExecutor.getRunnable(pf.get.pipeline).start(config)

}
