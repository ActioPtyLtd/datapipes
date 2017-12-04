package actio.datapipes.pipescript.Pipeline

case class Service(path: String, get: Option[Operation], put: Option[Operation], post: Option[Operation], patch: Option[Operation])
