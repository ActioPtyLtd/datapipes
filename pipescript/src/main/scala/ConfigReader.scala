import java.io.File

import Common.Data._
import Common.DataSet
import com.typesafe.config._

import scala.collection.JavaConversions._

object ConfigReader {

  def read(file: String): DataSet = convert(
    ConfigFactory.parseProperties(System.getProperties)
      .withFallback(ConfigFactory.parseFile(new File(file)))
      .resolve())

  def convert(config: Config): DataSet = convert("root", config.root())

  def convert(label: String, config: ConfigValue): DataSet =
    config match {
      case co: ConfigObject => DataRecord(label,co.map(o => convert(o._1, o._2)).toList)
      case cl: ConfigList => DataArray(label, cl.map(o => convert("item", o)).toList)
      case cv => config.unwrapped() match {
        case str: String => DataString(label,str)
        case int: Integer => DataNumeric(int)
      }
    }



}
