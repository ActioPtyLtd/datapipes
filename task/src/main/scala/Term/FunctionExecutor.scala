package Term

import java.lang.reflect.Parameter

import Common.Data._
import Common.DataSet
import Common.Data.ImplicitCasts._

import scala.annotation.tailrec
import scala.util.Try

object FunctionExecutor {

  def execute(methodName: String, params: List[DataSet]): DataSet =
    Class.forName("Term.Functions")
      .getDeclaredMethods
      .filter(f => f.getName.equalsIgnoreCase(methodName) && params.size >= f.getParameterCount)
      .map(m => (m,getParamValues(m.getParameters.toList zip params, Nil)))
      .find(c => c._2.isDefined)
      .flatMap(i =>
        i._1.invoke(null, i._2.get.map(_.asInstanceOf[Object]): _*) match {
          case r: DataSet => Some(r)
          case str: String => Some(str: DataSet)
          case bool: java.lang.Boolean => Some(bool: DataSet)
          case _ => None
        })
      .getOrElse(DataNothing())

  @tailrec
  private def getParamValues(parameters: List[(Parameter,DataSet)], result: List[Any]): Option[List[Any]] = parameters match {
    case Nil => Some(result.reverse)
    case ((methodParameter,DataString(_,str)) :: tail)
      if methodParameter.getType == classOf[String] => getParamValues(tail, Option(str).getOrElse("") :: result)
    case ((methodParameter,DataNumeric(_,num)) :: tail)
      if methodParameter.getType == classOf[Int] && Try(num.toInt).isSuccess => getParamValues(tail, num.toInt :: result)
    case _ => None
  }

}
