package actio.datapipes.pipescript.Pipeline

case class Pipeline(name: String, pipe: Operation, schedule: Option[Schedule])