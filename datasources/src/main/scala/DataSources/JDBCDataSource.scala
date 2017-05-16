package DataSources

import DataPipes.Common.Data._
import DataPipes.Common.{DataSource, Observer, Parameters}

import java.sql._

import com.typesafe.scalalogging.Logger

class JDBCDataSource extends DataSource {

  var _observer: Option[Observer[DataSet]] = None

  val logger = Logger("JDBCDataSource")

  def subscribe(observer: Observer[DataSet]): Unit = _observer = Some(observer)

  def exec(parameters: Parameters): Unit = {


    val connectionString = parameters("connect").stringOption.getOrElse("")

    val cn = DriverManager.getConnection(connectionString)

    logger.info("Connected...")

    val statement = parameters("query")("read").stringOption.getOrElse("")

    val stmt: PreparedStatement = cn.prepareStatement(statement)

    logger.info("Executing SQL batch statement...")
    logger.info(statement)

    val rs = stmt.executeQuery()

    val metaData = rs.getMetaData
    val ordinals = 1 to metaData.getColumnCount
    val header = ordinals.map(o => (metaData.getColumnType(o), metaData.getColumnName(o))).toList

    while (rs.next())
    {
      _observer.get.next(DataRecord("row", header.map(v =>
        JDBCDataSource.typeMap.get(v._1).map(m => m(v._2, rs)).getOrElse(DataString(v._2, rs.getObject(v._2).toString)))))
    }


    _observer.get.completed()

    logger.info("Successfully executed statement.")

    cn.close()
  }

}

object JDBCDataSource {

  lazy val typeMap = List(

    (List(Types.BIGINT, Types.DECIMAL, Types.DOUBLE, Types.FLOAT, Types.INTEGER, Types.NUMERIC),
      (name: String, rs: ResultSet) =>
        DataNumeric(name,
          BigDecimal(BigDecimal(rs.getObject(name).toString)
            .underlying()
            .stripTrailingZeros()
            .toPlainString))),

    (List(Types.DATE, Types.TIME, Types.TIMESTAMP),
      (name: String, rs: ResultSet) =>
        DataDate(name, rs.getDate(name)))

  ).flatMap(t => t._1.map(s => s -> t._2)).toMap

}