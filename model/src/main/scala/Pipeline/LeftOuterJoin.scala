package Pipeline

case class LeftOuterJoin(name: String, input: Operation, io: Operation) extends Operation
