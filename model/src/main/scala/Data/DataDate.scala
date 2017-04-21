package Data

import java.text.SimpleDateFormat

case class DataDate(label: String, date: java.util.Date) extends DataSet {

  override def stringOption: Option[String] = Some(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(date))
}

object DataDate {

  private val label = "date"

  def apply(date: java.util.Date): DataDate = DataDate(label, date)
}