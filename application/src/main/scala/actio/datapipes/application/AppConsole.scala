package actio.datapipes.application

import java.io.File
import java.util.UUID

import actio.datapipes.pipescript.Pipeline._
import actio.datapipes.pipeline.SimpleExecutor
import actio.datapipes.pipescript.ConfigReader
import com.typesafe.scalalogging.Logger
import org.apache.commons.cli._

object AppConsole {

  lazy val logger = Logger("AppTest")

  def main(args: Array[String]): Unit = {

    val parser: CommandLineParser = new DefaultParser()

    val options = new Options()
    options.addOption("c", "config", true, "config")
    options.addOption("p", "pipe", true, "run named pipeline ..")
    options.addOption("s", "service", false, "run as Service, as configured in Services section")
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

    if(line.hasOption("p"))
      System.setProperty("script.startup.exec",line.getOptionValue('p'))
    System.setProperty("runId", UUID.randomUUID().toString)
    System.setProperty("configName", new File(configFile).getName)

    logger.info(configFile)

    val config = ConfigReader.read(configFile)
    val pf = Builder.build(config)

    logger.info(s"Running pipe: ${pf.defaultPipeline}")

    if(line.hasOption("s"))
      new AppService(pf)
    else
      SimpleExecutor.getRunnable(pf.pipelines.find(f => f.name == pf.defaultPipeline).get).start(config)

    logger.info(s"Pipe ${pf.defaultPipeline} completed successfully.")
  }

}
