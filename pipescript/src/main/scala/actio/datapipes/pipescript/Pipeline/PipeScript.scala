package actio.datapipes.pipescript.Pipeline

import actio.common.Data.DataSet

case class PipeScript(settings: DataSet, services: List[Service], tasks: List[Operation], pipelines: List[Operation], defaultPipeline: String)