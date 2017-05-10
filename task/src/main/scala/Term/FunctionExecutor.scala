package Term

import java.lang.reflect.Parameter

import Common.Data._
import Common.DataSet
import Common.Data.ImplicitCasts._

import scala.annotation.tailrec
import scala.util.Try

object FunctionExecutor {

  def execute(methodName: String, params: List[DataSet]): DataSet =

    // Tries to invoke the best possible implementation of a function based on name, input types and parameter length
    // Also casts the return type to a DataSet
    Class.forName("Term.Functions")
      .getDeclaredMethods
      .filter(f => f.getName.equalsIgnoreCase(methodName) && params.size >= f.getParameterCount)
      .map(m => (m,getParamValues(m.getParameters.toList zip params, Nil)))
      .find(c => c._2.isDefined)
      .flatMap(i =>
        i._1.invoke(null, i._2.get.map(_.asInstanceOf[Object]): _*) match {   // return type conversion
          case r: DataSet => Some(r)
          case str: String => Some(str: DataSet)
          case bool: java.lang.Boolean => Some(bool: DataSet)
          case num: BigDecimal => Some(num: DataSet)
          case date: java.util.Date => Some(date: DataSet)
          case _ => None
        })
      .getOrElse(DataNothing())

  // pattern match on parameters and cast if necessary
  @tailrec
  private def getParamValues(parameters: List[(Parameter,DataSet)], result: List[Any]): Option[List[Any]] = parameters match {
    case Nil => Some(result.reverse)
    case ((methodParameter,DataString(_,str)) :: tail)
      if methodParameter.getType == classOf[String] => getParamValues(tail, Option(str).getOrElse("") :: result)
    case ((methodParameter,DataNumeric(_,num)) :: tail)
      if methodParameter.getType == classOf[Int] && Try(num.toInt).isSuccess => getParamValues(tail, num.toInt :: result)
    case ((methodParameter,DataNumeric(_,num)) :: tail)
      if methodParameter.getType == classOf[BigDecimal] => getParamValues(tail, num :: result)
    case ((methodParameter,DataDate(_,date)) :: tail)
      if methodParameter.getType == classOf[java.util.Date] => getParamValues(tail, date :: result)
    case _ => None
  }

}
