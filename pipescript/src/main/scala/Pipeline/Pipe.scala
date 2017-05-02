package Pipeline

case class Pipe(name: String, left: Operation, right: Operation) extends Operation
