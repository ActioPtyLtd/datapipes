package Pipeline

case class TransformLambda(name: String, input: Operation, lambdaExpression: Lambda) extends Operation