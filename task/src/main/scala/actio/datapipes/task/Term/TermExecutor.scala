package actio.datapipes.task.Term


import actio.common.Data._
import actio.datapipes.task.TaskSetting
import com.typesafe.scalalogging.Logger

import scala.meta._

class TermExecutor(taskSetting: TaskSetting) {

  val logger = Logger("Meta")

  def eval(ds: DataSet, text: String): DataSet = eval(ds, text.parse[Term].get)

  def getTemplateTerm(str: String) = ("s\"\"\"" + str + "\"\"\"").parse[Term].get

  def interpolate(str: String): String = "s\"\"\"" + str + "\"\"\""

  def eval(ds: DataSet, t: Term): DataSet = {
    try {
      return t match {

        // evaluate lambda expression
        case Term.Function(Seq(Term.Param(_, Term.Name(name), _, _)), body) =>
          eval(body, Map(name -> ds))

        // evaluate expression, add to scope top level attributes of the DataSet
        case _ => eval(t, ds.map(e => e.label -> e).toList.toMap + ("this" -> ds))
      }
    } catch {
      case e : Throwable =>
        logger.error(" MetaTerm Error:: Exception e "+e.toString)
        throw(e)
    }
  }

  def evalLambdas(t: Term, ds: Seq[DataSet]): DataSet = t match {
    case Term.Function(seq: Seq[Term.Param], body) => eval(body, (seq.map {
      case Term.Param(Nil, Term.Name(name), None, None) => name
      case _ => ""
    } zip ds).map(m => (m._1 -> m._2)).toMap)
  }


  def eval(t: AnyRef, scope: Map[String, AnyRef]): DataSet = t match {

    // construct literal string
    case Lit(str: String) => DataString(str)

    // construct literal numeric
    case Lit(int: Int) => DataNumeric(int)

    case Lit(bool: Boolean) => DataBoolean(bool)

    // tuple for dataset construction

    case Term.Tuple(Term.Name(label) +: tail) => DataRecord(label,
      tail.map(e => eval(e, scope)).toList)

    case Term.Tuple(s) => DataRecord(eval(s.head, scope).toString,
      s.tail.map(e => eval(e, scope)).toList)

    // for when you want to reference original top level dataset
    case Term.This(_) => scope("this") match {
      case ds: DataSet => ds
      case term => eval(term, scope)
    }

    // get variable in scope
    case Term.Name(name) => scope(name) match {
      case ds: DataSet => ds
      case term => eval(term, scope)
    }

    // evaluate conditional block
    case Term.If(cond, thenp, elsep) =>
      if (eval(cond, scope) match {
        case DataBoolean(_, bool) => bool
        case _ => false
      }) {
        eval(thenp, scope)
      } else {
        eval(elsep, scope)
      }

    // evaluate templates
    case Term.Interpolate(_, strings, terms) => DataString(
      (strings zip terms)
        .map(p => p._1.toString() + eval(p._2, scope).stringOption.getOrElse(""))
        .mkString + strings.last.toString)

    // support dot notation to access DataSets
    case select: Term.Select => evalSelect(select, scope)

    // call functions
    case apply: Term.Apply => evalApply(apply, scope)

    // evaluate infix operations
    case infix: Term.ApplyInfix => evalApplyInfix(infix, scope)

    // evaluate unary operations
    case unary: Term.ApplyUnary => evalApplyUnary(unary, scope)

    case e => throw new Exception(e.toString)
  }

  def evalSelect(t: Term.Select, scope: Map[String, AnyRef]): DataSet = t match {

    // Placeholder '_' will evaluate to accessing a
    // DataSet record by empty label (maybe obsolete)
    case Term.Select(q, Term.Placeholder()) => Operators.flatten(eval(q, scope))

    // Astrix should be treated as iteration
    case Term.Select(Term.Select(q, Term.Name("*")), Term.Name(n)) =>
      DataArray(eval(q, scope).map(i => i(n)).toList)

    // Look for the DataSet with label
    case Term.Select(q, Term.Name(n)) => {
      val res = eval(q, scope)
      if(!taskSetting.strictTraversal || res(n).isDefined)
        res(n)
      else
        throw new Exception(s"The field $n does not exist under the element ${res.label}.")
    }
  }

  def evalApply(t: Term.Apply, scope: Map[String, AnyRef]): DataSet = t match {

    // construct a fixed size DataArray
    case Term.Apply(Term.Name("Array"), args) =>
      DataArray(args.map(eval(_, scope)).toList)

    // construct a fixed size DataArray
    case Term.Apply(Term.Name("Record"), args) =>
      DataRecord(args.map(eval(_, scope)).toList)

    // dynamically call function, evaluating parameters before execution
    case Term.Apply(Term.Name(fName), args)
      if !scope.contains(fName) =>
        FunctionExecutor.execute(taskSetting.nameSpace, fName, args.map(eval(_, scope)).toList)

    // get DataSet by ordinal
    case Term.Apply(q, Seq(Lit(num: Int))) => eval(q, scope)(num)

    // get DataSet by name
    case Term.Apply(q, Seq(Lit(str: String))) => eval(q, scope)(str)

    // iterate through DataSet and include elements matching condition
    case Term.Apply(
    Term.Select(s, Term.Name("filter")),
    Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
      DataArray(
        eval(s, scope)
          .elems
          .filter(f => eval(rem, scope + (tn -> f)).asInstanceOf[DataBoolean].bool)
          .toList)

    // iterate through DataSet and exclude elements matching condition
    case Term.Apply(
    Term.Select(s, Term.Name("filterNot")),
    Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
      DataArray(
        eval(s, scope)
          .elems
          .filterNot(f => eval(rem, scope + (tn -> f)).asInstanceOf[DataBoolean].bool)
          .toList)

    // iterate through DataSet and apply function to each element
    case Term.Apply(
    Term.Select(s, Term.Name("map")),
    Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
      DataArray(
        eval(s, scope)
          .elems
          .map(f => eval(rem, scope + (tn -> f)))
          .toList)

    // iterate through DataSet and apply function to each element
    case Term.Apply(
    Term.Select(s, Term.Name("flatMap")),
    Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
      DataArray(
        eval(s, scope)
          .elems
          .flatMap(f => eval(rem, scope + (tn -> f)).elems)
          .toList)

    case Term.Apply(
    Term.Select(s, Term.Name("groupBy")),
    Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
      DataRecord(
        eval(s, scope)
          .elems
          .toList
          .groupBy(k => eval(rem, scope + (tn -> k)).stringOption.getOrElse(""))
          .map(p => DataArray(p._1, p._2))
          .toList)

    case Term.Apply(
    Term.Select(s, Term.Name("find")),
    Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
      eval(s, scope)
        .elems
        .toList
        .find(f =>
          eval(rem, scope + (tn -> f)) match {
            case DataBoolean(_, bool) => bool
            case _ => false
          })
        .getOrElse(DataNothing())

    case Term.Apply(
    Term.Select(s, Term.Name("reduceLeft")),
    Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(ta), None, None), Term.Param(Nil, Term.Name(tb), None, None)), rem))) => {
      val array = eval(s, scope)

      if (array.elems.toList.isEmpty)
        array
      else
        array
          .elems
          .toList
          .reduceLeft((a, b) => eval(rem, scope + (ta -> a) + (tb -> b)))
    }

    // convert data set to json string
    case Term.Apply(Term.Select(t, Term.Name("toJson")), Nil) => {
      import actio.common.Data.JsonXmlDataSet.Extend
      DataString(eval(t, scope).toJson)
    }

    case Term.Apply(Term.Select(t, Term.Name("parseJson")), Nil) => {
      JsonXmlDataSet.fromJson(eval(t, scope).stringOption.getOrElse(""))
    }

    case Term.Apply(Term.Select(t, Term.Name("label")), Nil) => {
      val l = eval(t, scope).label
      DataString(l,l)
    }


    // convert data set to xml string
    case Term.Apply(Term.Select(t, Term.Name("toXml")), Nil) => {
      import actio.common.Data.JsonXmlDataSet.Extend
      DataString(eval(t, scope).toXml)
    }

    // check to see if data set is empty
    case Term.Apply(Term.Select(t, Term.Name("isEmpty")), Nil) =>
      DataBoolean(eval(t, scope).isEmpty)

    // check if data set is nothing
    case Term.Apply(Term.Select(t, Term.Name("isDefined")), Nil) =>
      DataBoolean(eval(t, scope).isDefined)

    // check the size of the data set
    case Term.Apply(Term.Select(t, Term.Name("size")), Nil) =>
      DataNumeric(eval(t, scope).size)

    case Term.Apply(Term.Select(t, Term.Name("toString")), Nil) =>
      DataString(eval(t, scope).stringOption.getOrElse(""))

    // check the size of the data set
    case Term.Apply(Term.Select(t, Term.Name("flatten")), Nil) => Operators.flatten(eval(t, scope))

    // get DataSet by name
    case Term.Apply(q, Seq(t)) => eval(q, scope)(eval(t, scope).stringOption.getOrElse(""))

  }

  // reverse sign only if the term evaluates to a numeric
  def evalApplyUnary(t: Term.ApplyUnary, scope: Map[String, AnyRef]): DataSet = t match {

    case Term.ApplyUnary(Term.Name("-"), r) => eval(r, scope) match {
      case DataNumeric(label, num) => DataNumeric(label, -num)
      case _ => DataNothing()
    }

    case Term.ApplyUnary(Term.Name("!"), r) => eval(r, scope) match {
      case DataBoolean(label, b) => DataBoolean(label,!b)
      case _ => DataNothing()
    }
  }

  def evalApplyInfix(t: Term.ApplyInfix, scope: Map[String, AnyRef]): DataSet = t match {

    // >=
    case Term.ApplyInfix(l, Term.Name(">="), Nil, Seq(r: Term)) =>
      eval(l, scope) match {
        case d: DataDate =>
          DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) >= 0)
        case n: DataNumeric =>
          DataBoolean(n.num >= eval(r, scope).asInstanceOf[DataNumeric].num)
        case _ =>
          DataBoolean(false)
      }

    // >
    case Term.ApplyInfix(l, Term.Name(">"), Nil, Seq(r: AnyRef)) =>
      eval(l, scope) match {
        case d: DataDate =>
          DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) > 0)
        case n: DataNumeric =>
          DataBoolean(n.num > eval(r, scope).asInstanceOf[DataNumeric].num)
        case _ =>
          DataBoolean(false)
      }

    // <
    case Term.ApplyInfix(l, Term.Name("<"), Nil, Seq(r: AnyRef)) => eval(l, scope) match {
      case d: DataDate =>
        DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) < 0)
      case n: DataNumeric =>
        DataBoolean(n.num < eval(r, scope).asInstanceOf[DataNumeric].num)
      case _ =>
        DataBoolean(false)
    }

    // <=
    case Term.ApplyInfix(l, Term.Name("<="), Nil, Seq(r: AnyRef)) => eval(l, scope) match {
      case d: DataDate =>
        DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) <= 0)
      case n: DataNumeric =>
        DataBoolean(n.num <= eval(r, scope).asInstanceOf[DataNumeric].num)
      case _ =>
        DataBoolean(false)
    }

    // string concat and numeric addition
    case Term.ApplyInfix(l, Term.Name("+"), Nil, Seq(r: AnyRef)) => (eval(l, scope), eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(left.num + right.num)
      case (left, right) => DataString(left.toString +
        right.toString)
    }

    // subtract numeric
    case Term.ApplyInfix(l, Term.Name("-"), Nil, Seq(r: AnyRef)) => (eval(l, scope), eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(
        left.num -
          right.num)
    }

    // multiply numeric
    case Term.ApplyInfix(l, Term.Name("*"), Nil, Seq(r: AnyRef)) => (eval(l, scope), eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(
        left.num *
          right.num)
      case _ => DataNothing()
    }

    // divide numeric
    case Term.ApplyInfix(l, Term.Name("/"), Nil, Seq(r: AnyRef)) => (eval(l, scope), eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(
        left.num /
          right.num)
      case _ => DataNothing()
    }

    // currently, equality will do a string comparison
    case Term.ApplyInfix(l, Term.Name("=="), Nil, Seq(r: AnyRef)) => (eval(l, scope), eval(r, scope)) match {
      case (ls: DataString, rs: DataString) => DataBoolean(ls.str == rs.str)
      case (ls: DataNumeric, rs: DataNumeric) => DataBoolean(ls.num == rs.num)
      case (ls: DataBoolean, rs: DataBoolean) => DataBoolean(ls.bool == rs.bool)
      case (ls: DataDate, rs: DataDate) => DataBoolean(ls.date == rs.date)
      case (ls: DataSet, rs: DataSet) => DataBoolean(ls.stringOption.isDefined && rs.stringOption.isDefined && ls.stringOption.get == rs.stringOption.get)
    }

    // shameless copy & paste
    case Term.ApplyInfix(l, Term.Name("!="), Nil, Seq(r: AnyRef)) => (eval(l, scope), eval(r, scope)) match {
      case (ls: DataString, rs: DataString) => DataBoolean(ls.str != rs.str)
      case (ls: DataNumeric, rs: DataNumeric) => DataBoolean(ls.num != rs.num)
      case (ls: DataBoolean, rs: DataBoolean) => DataBoolean(ls.bool != rs.bool)
      case (ls: DataDate, rs: DataDate) => DataBoolean(ls.date != rs.date)
      case (ls: DataSet, rs: DataSet) => DataBoolean(ls.stringOption.isDefined && rs.stringOption.isDefined && ls.stringOption.get != rs.stringOption.get)
    }
    // AND logic
    case Term.ApplyInfix(l, Term.Name("&&"), Nil, Seq(r: AnyRef)) => {
      val ls = eval(l, scope).asInstanceOf[DataBoolean]

      if (ls.bool) {
        eval(r, scope)
      } else {
        DataBoolean(false)
      }
    }

    // OR logic
    case Term.ApplyInfix(l, Term.Name("||"), Nil, Seq(r: AnyRef)) => {
      val ls = eval(l, scope).asInstanceOf[DataBoolean]

      if (ls.bool) {
        DataBoolean(true)
      } else {
        eval(r, scope)
      }
    }

    case Term.ApplyInfix(l, Term.Name("mergeLeft"), Nil, Seq(r: AnyRef)) => {
      Operators.mergeLeft(eval(l, scope), eval(r, scope))
    }

    case Term.ApplyInfix(Term.Name(key), Term.Name("->"), Nil, Seq(r: AnyRef)) => {
      val value = eval(r, scope)
      value match {
        case DataString(_, v) => DataString(key, v)
        case DataNumeric(_, v) => DataNumeric(key, v)
        case DataBoolean(_,b) => DataBoolean(key, b)
        case DataRecord(_,v) => DataRecord(key, v)
        case DataArray(_,v) => DataArray(key, v)
        case DataDate(_,v) => DataDate(key, v)
        case DataNothing(_) => DataNothing(key)
        case _ => DataString(key, "")
      }
    }

    case Term.ApplyInfix(l, Term.Name("->"), Nil, Seq(r: AnyRef)) => {
      val key = eval(l, scope).toString
      val value = eval(r, scope)
      value match {
        case DataString(_, v) => DataString(key, v)
        case DataNumeric(_, v) => DataNumeric(key, v)
        case DataBoolean(_,b) => DataBoolean(key, b)
        case DataRecord(_,v) => DataRecord(key, v)
        case DataArray(_,v) => DataArray(key, v)
        case DataDate(_,v) => DataDate(key, v)
        case DataNothing(_) => DataNothing(key)
        case _ => DataString(key, "")
      }
    }


  }

}
