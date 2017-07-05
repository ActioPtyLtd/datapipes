package actio.datapipes.pipescript

import java.io.{File, FileNotFoundException}

import DataPipes.Common.Data.{DataSet, _}
import com.typesafe.config._

import scala.collection.JavaConversions._

object ConfigReader {

  def read(file: String): DataSet = {
    val f = new File(file)
    if(f.canRead)
      convert(
        ConfigFactory.parseProperties(System.getProperties)
          .withFallback(ConfigFactory.parseFile(f))
          .resolve())
    else
      throw new FileNotFoundException(file)
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
