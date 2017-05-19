
import Pipeline._
import org.apache.commons.cli._

object AppTest {

  def main(args: Array[String]): Unit = {

    val parser: CommandLineParser = new DefaultParser();

    val options = new Options()
    options.addOption("c", "config", true, "config")
    //options.addOption("p", "pipe", true, "run named pipeline ..")
    //options.addOption("s", "service", false, "run as Service, as configured in Services section")
    //options.addOption("n", "return number of records processed in final task as the exit code")
    //options.addOption("help", "print this help message")
    //options.addOption("L", "load config file into Admin Server")
    //options.addOption("S", "Supress event streaming to Admin Server")
    //options.addOption(Option.builder("D").argName("property=value").hasArgs.valueSeparator('=').build)

    val line = parser.parse(options, args)

    val configFile =
      if (line.hasOption("c")) { // print the value of config
        line.getOptionValue('c')
      } else {
        "./application.conf"
      }

    println(configFile)

    val config = ConfigReader.read(configFile)
    val pf = Builder.build(config)

    println(pf)

    SimpleExecutor.getRunnable(pf.get.pipeline).start(config)
  }

}
