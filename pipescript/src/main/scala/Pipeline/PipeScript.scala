package Pipeline

import DataPipes.Common.Data._

case class PipeScript(settings: DataSet, tasks: List[Operation], pipeline: Operation)