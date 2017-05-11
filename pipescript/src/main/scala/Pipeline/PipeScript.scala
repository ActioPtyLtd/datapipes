package Pipeline

import Common.DataSet

case class PipeScript(settings: DataSet, tasks: List[Operation], pipeline: Operation)