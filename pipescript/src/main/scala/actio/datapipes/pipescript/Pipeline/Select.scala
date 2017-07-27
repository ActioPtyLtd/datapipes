package actio.datapipes.pipescript.Pipeline

case class Select(left: Operation, field: String, name: String = "select") extends Operation