package actio.datapipes.application

import java.io.File
import java.util.UUID

import actio.common.Data._
import actio.common.{Dom, Event, EventAssertionFailed}
import actio.datapipes.dataSources.TusDataSource
import actio.datapipes.pipescript.Pipeline._
import actio.datapipes.pipeline.SimpleExecutor
import actio.datapipes.pipeline.SimpleExecutor.pipelineRunId
import actio.datapipes.pipescript.ConfigReader
import com.typesafe.scalalogging.Logger
import org.apache.commons.cli._
import org.apache.commons.io.FilenameUtils

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
    options.addOption("R", "Read config from REST service")
    options.addOption("u", "Upload files only")
    options.addOption("U", "Upload files after run")
    //options.addOption("S", "Supress event streaming to Admin Server")
    //options.addOption(Option.builder("D").argName("property=value").hasArgs.valueSeparator('=').build)

    val line = parser.parse(options, args)

    val configFile =
      if (line.hasOption("c")) { // print the value of config
        line.getOptionValue('c')
      } else {
        "./application.conf"
      }

    if (line.hasOption("p"))
      System.setProperty("script.startup.exec", line.getOptionValue('p'))
    if (System.getProperty("run.id") == null)
      System.setProperty("run.id", UUID.randomUUID().toString)
    System.setProperty("run.configName", FilenameUtils.removeExtension(new File(configFile).getName))
    System.setProperty("run.pipeName", if (line.hasOption("p")) line.getOptionValue('p') else "default")

    import java.text.SimpleDateFormat
    val dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss")

    System.setProperty("run.startDate", dateFormat.format(new java.util.Date()))

    logger.info(configFile)
    logger.info(s"run.id=${System.getProperty("run.id")}")

    val config = ConfigReader.read(configFile)
    val pipeScript = PipeScriptBuilder.build(configFile, config)

    if (line.hasOption("u")) {
      syncFiles(config)
      Runtime.getRuntime.exit(0)
    } else if (line.hasOption("s")) {
      logger.info(s"Running data pipes as a service on port ${pipeScript.settings("port").intOption.getOrElse(8080)}.")
      // this is a workaround because HEAD requests were magically converted to GET requests
      System.setProperty("akka.http.server.transparent-head-requests", "false")
      new AppService(pipeScript)
    }
    else {
      Scheduler.boot(configFile, DataArray(config))

      if (line.hasOption("U")) {
        syncFiles(config)
      }

      Runtime.getRuntime.exit(0)
    }
  }

  def syncFiles(config: DataSet): Unit = {
    import actio.datapipes.dataSources.RESTJsonDataSource

    logger.info("Authenticating Agent...")
    val response = new RESTJsonDataSource().executeQuery(config("actio_auth"), config("actio_auth")("query")("read"))
    val token = response("body")("access_token").toString
    logger.info(s"Token received: $token")

    val query = Operators.mergeLeft(
      config("actio_sync")("query")("create"),
      DataRecord(DataRecord("headers", DataString("access_token", token)))
    )

    new TusDataSource().execute(config("actio_sync"), query)
  }

}
