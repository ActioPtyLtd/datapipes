package Data

import Common._

object PrettyPrint {

  implicit class PrettyPrint(data: Data) {
    def print(): String = data match {
      case DataString(l, s) => l + " -> \"" + s + "\""

      case DataRecord(key, fs) =>
        "(" + key + "," +
          fs.map(f => f.print()).mkString(",") +
          ")"
      case DataArray(key, fs) =>
        "[" + key + "," +
          fs.map(f => f.print()).mkString(",") +
          "]"
      case DataNothing(_) => "()"
      case DataNumeric(l, num) => l + " -> " + num.setScale(2, BigDecimal.RoundingMode.HALF_UP).underlying().stripTrailingZeros().toPlainString
      case DataBoolean(l, bool) => l + " -> " + bool.toString
      case DataDate(l, date) => l + " -> " + date.toString
    }
  }

}