package actio.datapipes.pipescript.Pipeline

import actio.common.Data.DataSet

case class PipeScript(
  name: String,
  settings: DataSet,
  services: List[Service],
  tasks: List[Operation],
  pipelines: List[Pipeline],
  schedule: Option[Schedule],
  defaultPipeline: String
)