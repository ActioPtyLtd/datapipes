package actio.datapipes.pipescript.Pipeline

trait Sequence extends Operation {
  def name: String
  def left: Operation
  def right: Operation
}

case class SequenceOnFailOrSuccess(name: String, left: Operation, right: Operation) extends Sequence
case class SequenceOnSuccess(name: String, left: Operation, right: Operation) extends Sequence
