package actio.common.Data

object Operators {

  def mergeLeft(l: DataSet, r: DataSet): DataSet = mergeLeft(l, r, r.label)

  def mergeLeft(l: DataSet, r: DataSet, newLabel: String): DataSet = {

    if (r.elems.isEmpty)
      l
    else {
      val head = r.elems.head
      val tail = r.elems.tail.toList
      val findLabel = l.elems.find(f => f.label == head.label)

      if (findLabel.isEmpty) {
        mergeLeft(DataRecord(l.label, head :: l.elems.toList), DataRecord(r.label, tail), newLabel)
      }
      else {
        (findLabel, head) match {
          case (Some(f@DataRecord(_, _)), DataRecord(_, _)) =>
            mergeLeft(DataRecord(l.label, mergeLeft(f, head, newLabel)
              :: l.elems.filterNot(f => f.label == head.label).toList), DataRecord(r.label, tail), newLabel)
          case (Some(f@DataArray(_,_)), DataArray(_,_)) =>
            mergeLeft(DataRecord(l.label, DataArray(head.label, (head.elems ++ f.elems).toList)
              :: l.elems.filterNot(f => f.label == head.label).toList), DataRecord(r.label, tail), newLabel)
          case _ =>
            mergeLeft(l, DataRecord(r.label, DataRecord(newLabel, List(head)) :: tail), newLabel)
        }
      }
    }
  }

  //def append(l: DataSet, r: DataSet): DataSet = {

  //}

  def flatten(ds: DataSet): DataSet = {
    val children = ds match {
      case DataRecord(_,r) => r.flatMap(e => flatten(e).elems)
      case DataArray(_,a) => a.flatMap(e => flatten(e).elems)
      case _ => List(ds)
    }
    DataRecord(children.groupBy(g => g.label).map(m => m._2.head).toList)
  }

  def relabel(ds: DataSet, key: String): DataSet = ds match {
    case DataString(_, v) => DataString(key, v)
    case DataNumeric(_, v) => DataNumeric(key, v)
    case DataDate(_, d) => DataDate(key, d)
    case DataBoolean(_, b) => DataBoolean(key, b)
    case DataRecord(_, f) => DataRecord(key, f)
    case DataArray(_, a) => DataArray(key, a)
    case _ => DataString(key, "")
  }

  def minus(ds1: DataSet, ds2: DataSet): Option[DataSet] = (ds1,ds2) match {
    case (r1 @ DataRecord(l1,f1),r2 @ DataRecord(l2,f2)) if l1 == l2 => {
      val list = f1.flatMap(f => minus(f,r2(f.label)))
      if(list.isEmpty)
        None
      else
        Some(DataRecord(l1, list))
    }
    case _ if ds1 == ds2 => None
    case _ => Some(ds1)
  }
}
