package Pipeline

import DataPipes.Common.Data._

case class PipeScript(settings: DataSet, tasks: List[Operation], pipelines: List[Operation], defaultPipeline: String)