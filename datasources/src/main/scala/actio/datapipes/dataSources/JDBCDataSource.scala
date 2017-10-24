package actio.datapipes.dataSources

import java.sql._

import actio.common.Data._
import actio.common.{DataSource, Observer}
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer

class JDBCDataSource extends DataSource {

  val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  val logger = Logger("JDBCDataSource")

  def subscribe(observer: Observer[DataSet]): Unit = _observer.append(observer)

  def execute(config: DataSet, statement: String, executeQuery: Boolean): Unit = {
    val connectionString = config("connect").stringOption.getOrElse("")

    val cn = DriverManager.getConnection(connectionString)

    logger.info("Connected.")

    val stmt: PreparedStatement = cn.prepareStatement(statement)

    logger.info("Executing SQL statement...")
    logger.info(statement.substring(0, Math.min(statement.length, 100)) + (if(statement.length>100) "..." else ""))

    if(executeQuery) {
      val rs = stmt.executeQuery()

      val metaData = rs.getMetaData
      val ordinals = 1 to metaData.getColumnCount
      val cols = JDBCDataSource.uniqueNames(ordinals.map(metaData.getColumnName).toList, Nil)
      val header = (ordinals zip cols).map(o => (o._1, metaData.getColumnType(o._1), o._2)).toList

      while (rs.next()) {
        _observer.foreach(o => o.next(DataRecord("row", header.map(v =>
          if(rs.getObject(v._1) == null) DataNothing(v._3) else
          JDBCDataSource.typeMap.get(v._2).map(m => m(v._3, v._1, rs)).getOrElse(DataString(v._3, rs.getObject(v._1).toString))))))
      }
    } else {
      stmt.execute()
    }

    _observer.foreach(o => o.completed())

    logger.info("Successfully executed SQL statement.")

    cn.close()
  }

  override def execute(config: DataSet, query: DataSet*): Unit = {
    val read = query.headOption.map(_.label).contains("read")
    execute(config, query.flatMap(_.stringOption).mkString(";"), read)
  }

  def getCreateTableStatement(config: DataSet, statement: String): String = {
    val connectionString = config("connect").stringOption.getOrElse("")

    val cn = DriverManager.getConnection(connectionString)

    logger.info("Connected.")

    val stmt: PreparedStatement = cn.prepareStatement(statement)

    logger.info("Executing SQL statement...")
    logger.info(statement.substring(0, Math.min(statement.length, 100)) + (if(statement.length>100) "..." else ""))

    val rs = stmt.executeQuery()

    val metaData = rs.getMetaData
    val ordinals = 1 to metaData.getColumnCount

    val cols = ordinals.map(o => (metaData.getColumnName(o), metaData.getColumnTypeName(o))).toList
    logger.info("Schema retrieved.")

    cn.close()

    cols.map(m => "\"" + m._1 + "\" " + m._2).mkString(",")

  }
}

object JDBCDataSource {

  lazy val typeMap = List(

    (List(Types.BIGINT, Types.DECIMAL, Types.DOUBLE, Types.FLOAT, Types.INTEGER, Types.NUMERIC),
      (name: String, index: Int, rs: ResultSet) =>
        DataNumeric(name,
          BigDecimal(BigDecimal(rs.getObject(index).toString)
            .underlying()
            .stripTrailingZeros()
            .toPlainString))),

    (List(Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE),
      (name: String, index: Int, rs: ResultSet) =>
        DataDate(name, rs.getDate(index)))

  ).flatMap(t => t._1.map(s => s -> t._2)).toMap

  def uniqueNames(cols: List[String], result: List[(String,String)]): List[String] = cols match {
    case Nil => result.map(m => m._1 + m._2).reverse
    case c::cs => {
      val cnt = result.count(_._1 == c)

      uniqueNames(cs, (c, if (cnt == 0) "" else cnt.toString) :: result)
    }
  }

}