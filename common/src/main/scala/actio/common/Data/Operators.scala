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
          case _ =>
            mergeLeft(l, DataRecord(r.label, DataRecord(newLabel, List(head)) :: tail), newLabel)
        }
      }
    }
  }
}
