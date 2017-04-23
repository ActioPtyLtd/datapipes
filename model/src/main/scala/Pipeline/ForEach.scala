package Pipeline

// for each element in data execute io and merge the result
case class ForEach(name: String, input: Operation, io: Operation) extends Operation
