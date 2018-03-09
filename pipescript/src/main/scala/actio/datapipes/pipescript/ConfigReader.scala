package actio.datapipes.pipescript

import java.io.{File, FileNotFoundException}

import actio.common.Data._
import com.typesafe.config._

import scala.collection.JavaConversions._

object ConfigReader {

  def read(file: File): DataSet = {
    if(file.canRead)
      convert(
        ConfigFactory.parseProperties(System.getProperties)
          .withFallback(ConfigFactory.parseFile(file))
          .resolve())
    else
      throw new FileNotFoundException(file.toString)
  }

  def read(file: String): DataSet = {
    val f = new File(file)
    read(f)
  }

  def parse(content: String): DataSet = {
    convert(
      ConfigFactory.parseProperties(System.getProperties)
        .withFallback(ConfigFactory.parseString(content))
        .resolve())
  }

  def read(file: File, run: String): DataSet = {
    convert(
      ConfigFactory
        .parseString(run)
        .withFallback(ConfigFactory.parseFile(file))
        .resolve())
  }

  def readfromConfigList(config: Config, str: List[String]): Config = str match {
    case Nil => config
    case (h::t) => readfromConfigList(config.withFallback(ConfigFactory.parseString(h)), t)
  }

  def readfromConfigList(str: List[String]): DataSet = {
    convert(
      readfromConfigList(ConfigFactory.parseProperties(System.getProperties), str)
        .resolve()
    )
  }

  def convert(config: Config): DataSet = convert("root", config.root())

  def convert(label: String, config: ConfigValue): DataSet =
    config match {
      case co: ConfigObject => DataRecord(label,co.map(o => convert(o._1, o._2)).toList)
      case cl: ConfigList => DataArray(label, cl.map(o => convert("item", o)).toList)
      case _ => config.unwrapped() match {
        case str: String => DataString(label,str)
        case int: Integer => DataNumeric(label, int)
      }
    }



}
