package actio.datapipes.application

import java.io.File
import java.util.UUID

import actio.common.Data._
import actio.common.{Dom, Event}
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

    if(line.hasOption("p"))
      System.setProperty("script.startup.exec",line.getOptionValue('p'))
    if(System.getProperty("run.id") == null)
      System.setProperty("run.id", UUID.randomUUID().toString)
    System.setProperty("run.configName", FilenameUtils.removeExtension(new File(configFile).getName))
    System.setProperty("run.pipeName", if (line.hasOption("p")) line.getOptionValue('p') else "default")

    import java.text.SimpleDateFormat
    val dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss")

    System.setProperty("run.startDate", dateFormat.format(new java.util.Date()))

    logger.info(configFile)
    logger.info(s"run.id=${System.getProperty("run.id")}")

    val config = ConfigReader.read(configFile)

    val executeConfig =
      if(line.hasOption("R")) {
        val configs = downloadConfig(config("actio_home")).elems.toList
          .flatMap(r => r("config").stringOption)

        if (configs.isEmpty)
          DataNothing()
        else
          ConfigReader.readfromConfigList(configs)
      }
      else
        config

    if(executeConfig.isDefined && line.hasOption("u")) {
      syncFiles(executeConfig)
      Runtime.getRuntime.exit(0)
    }

    if(executeConfig.isDefined) {

      val pf = Builder.build(executeConfig)

      logger.info(s"Running pipe: ${pf.defaultPipeline}")

      if (line.hasOption("s")) {
        logger.info(s"Runing data pipes as a service on port ${pf.settings("port").intOption.getOrElse(8080)}.")
        // this is a workaround because HEAD requests were magically converted to GET requests
        System.setProperty("akka.http.server.transparent-head-requests", "false")
        new AppService(pf)
      }
      else {
        val startPipeline = pf.pipelines.find(f => f.name == pf.defaultPipeline).get
        val eventPipeline = pf.pipelines.find(f => f.name == "p_events")
          .map(e => (events: List[Event]) => SimpleExecutor.getRunnable(e, None)
            .next(Dom() ~ Dom("start", Nil, executeConfig, DataNothing(), Nil) ~
              Dom("event", Nil, DataArray(events.map(Event.toDataSet)), DataNothing(), Nil)))

        // send start event
        eventPipeline.foreach { ep =>
          ep(List(Event.runStarted()))
        }

        // run the main pipeline
        SimpleExecutor.getRunnable(startPipeline, eventPipeline).start(DataArray(executeConfig))

        if(line.hasOption("U")) {
          syncFiles(executeConfig)
        }

        // send the finish event
        eventPipeline.foreach { ep =>
          ep(List(Event.runCompleted()))
        }
      }

      logger.info(s"Pipe ${pf.defaultPipeline} completed successfully.")
    }
  }

  def downloadConfig(config: DataSet) : DataSet = {
    import actio.datapipes.dataSources.RESTJsonDataSource

    new RESTJsonDataSource().executeQuery(config, config("query")("read"))("body")
  }

  def syncFiles(config: DataSet) = {
    import actio.datapipes.dataSources.RESTJsonDataSource

    logger.info("Authenticating Agent...")
    val response = new RESTJsonDataSource().executeQuery(config("actio_auth"), config("actio_auth")("query")("read"))
    val token = response("body")("access_token").toString
    logger.info(s"Token received: ${token}")

    val query = Operators.mergeLeft(
      config("actio_sync")("query")("create"),
      DataRecord(DataRecord("headers", DataString("access_token", token))))

    new TusDataSource().execute(config("actio_sync"), config("actio_sync")("query")("create"))
  }

}
