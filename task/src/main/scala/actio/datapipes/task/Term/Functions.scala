package actio.datapipes.task.Term

import java.lang._
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import actio.common.Data._
import org.apache.commons.csv.CSVFormat

import scala.util.Try

object Functions {

  /* === string === */

  def toUpperCase(str: String): String = str.toUpperCase

  def toLowerCase(str: String): String = str.toLowerCase

  def trim(str: String): String = str.trim

  def substring(str: String, start: Int): String =
    if (start < str.length)
      str.substring(start)
    else
      ""

  def contains(str: String, targetStr: String): Boolean =
    if (str == null || targetStr == null)
      false
    else
      str.contains(targetStr)

  def replaceAll(str: String, find: String, replaceWith: String) = str.replaceAll(find, replaceWith)

  def split(str: String, s: String): Array[String] = str.split(s)

  def concat(strArray: Array[String]): String = concat(strArray, ",")

  def concat(strArray: Array[String], separator: String): String = {
    strArray.mkString(separator)
  }


  def csv(str: String, delim: String): DataSet = {
    import collection.JavaConverters._

    val parser = CSVFormat.RFC4180.withFirstRecordAsHeader().withDelimiter(delim.toCharArray.headOption.getOrElse(',')).parse(new java.io.StringReader(str))

    val records = parser.getRecords().asScala

    DataArray(records.map(r => DataRecord(parser.getHeaderMap().asScala.take(r.size()).map(c =>
      DataString(c._1, r.get(c._2))).toList)).toList)
  }


  def getProperty(str: String): String = System.getProperty(str)

  def sha256(str: String): String = org.apache.commons.codec.digest.DigestUtils.sha256Hex(str)

  /* === numeric === */

  def numeric(str: String, default: BigDecimal): BigDecimal = Try(BigDecimal(str)).getOrElse(default)

  def numeric(str: String): BigDecimal = numeric(str, 0)

  def numeric(str: String, format: String): String = numeric(str, format, "")

  def numeric(str: String, format: String, default: String): String = Try(new DecimalFormat(format).format(numeric(str))).getOrElse(default)

  /* === date === */

  def date(date: Date, format: String): String = new SimpleDateFormat(format).format(date)

  def dateParse(dateStr: String, formatIn: String, default: String): Date = {
    try {
      new SimpleDateFormat(formatIn).parse(dateStr)
    }
    catch {
      case e: Exception => new SimpleDateFormat(formatIn).parse(default)
    }
  }

  def now(): Date = new Date()

  def plusDays(date: Date, days: Int): Date = {
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, days)
    c.getTime
  }

  /* === short hand === */

  def sq(str: String): String = str.replace("'", "''")

}
