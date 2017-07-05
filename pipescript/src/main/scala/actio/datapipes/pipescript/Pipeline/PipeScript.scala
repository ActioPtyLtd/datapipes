package actio.datapipes.pipescript.Pipeline

import actio.common.Data.DataSet

case class PipeScript(settings: DataSet, tasks: List[Operation], pipelines: List[Operation], defaultPipeline: String)