package Pipeline

case class Duplicate(name: String, input: Operation, duplicate: List[Operation]) extends Operation