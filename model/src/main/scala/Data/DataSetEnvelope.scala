package Data

import Common._

case class DataSetEnvelope(data: SimpleIterator[DataSet], message: DataSet, error: DataSet)