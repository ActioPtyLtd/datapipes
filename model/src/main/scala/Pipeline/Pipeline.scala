package Pipeline

case class Pipeline(name: String, pipes: List[Operation]) extends Operation
