package Term.Legacy

import java.text.{DecimalFormat, SimpleDateFormat}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Date

import DataPipes.Common._
import DataPipes.Common.Data._
import org.apache.commons.lang.time.DateUtils

import scala.annotation.tailrec
import scala.util.Try

object Functions {

  /**
    * Sorts the DataSet by a specified property - Currently only supports dates
    *
    * @param ds         the DataSet containing the items to sort
    * @param property   the property to sort by
    * @param dataType   the data type of the property to sort by
    * @param dataFormat the format of the property value
    * @param direction  the direction to sort by
    * @return           a new DataSet containing the sorted items
    */



  def orderBy(ds: DataSet, property: String, dataType: String, dataFormat: String,  direction: String): DataSet = {
    orderByRE(ds, property, dataType, dataFormat, direction, 0)
  }

  def orderByRE(ds: DataSet, property: String, dataType: String, dataFormat: String,  direction: String, level: Int): DataSet = {
    @tailrec
    var dataFormatOption = Option(dataFormat)
    var orderedSet = ds
    if(dataFormat == "")
      dataFormatOption = None
    val elementCount = ds.elems.length
    if(elementCount > 1)
    {
      if(ds.elems.forall(x => x(property).toOption.isDefined)) {
        orderedSet = ds match {
          case DataArray(label, arrayElems) => dataType.toLowerCase() match {
            case "date" =>
              if (dataFormatOption.isDefined)
                DataArray(label, sortByDate(arrayElems, property, dataFormatOption.get, direction))
              else
                ds
            case _ => ds
          }
          case DataRecord(label, elements) => dataType.toLowerCase() match {
            case "date" =>
              if (dataFormatOption.isDefined)
                DataRecord(label, sortByDate(elements, property, dataFormatOption.get, direction))
              else
                ds
            case _ => var sortedElements = elements.sortBy(x => x(property).stringOption)
              if (direction.equalsIgnoreCase("desc"))
                sortedElements = sortedElements.reverse
              DataArray(label, sortedElements)
          }
          case _ => ds
        }
      }
      else
        orderedSet = DataRecord("item", ds.elems.toList)
    }
    else if (elementCount == 1) {
      if(ds.headOption.isDefined) {
        if (level == 0) {
          orderedSet = ds match {
            case DataArray(label, _) => DataArray(label, List[DataSet] {
              orderByRE(ds.headOption.get, property, dataType, dataFormat, direction, (level + 1))
            })
            case DataRecord(label, _) => DataRecord(label, List[DataSet] {
              orderByRE(ds.headOption.get, property, dataType, dataFormat, direction, (level + 1))
            })
            case DataSetHttpResponse(label, _, _, _, body) => DataRecord(label, List[DataSet] {
              orderByRE(body, property, dataType, dataFormat, direction, (level + 1))
            })
            case _ => ds
          }
        }
      }
    }
    if (level == 0)
      DataRecord("orderBy", List[DataSet]{orderedSet})
    else
      orderedSet
  }



  def sortByDate(items: List[DataSet], property: String, dateFormat: String, direction: String): List[DataSet] = {
    implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)
    val sortedItems = items.sortBy(x => LocalDate.parse(x(property).stringOption.getOrElse(""), DateTimeFormatter.ofPattern(dateFormat)).toEpochDay)(Ordering[Long])
    if(direction.equalsIgnoreCase("desc"))
      sortedItems.reverse
    else
      sortedItems
  }



  def take(ds: DataSet, numberOfItems: Int): DataSet = {
    val elementCount = ds.elems.length
    var takenSet = ds
    if(elementCount > 1)
      takenSet = DataArray(ds.label, ds.elems.take(numberOfItems).toList)
    else if(elementCount == 1 && ds.headOption.isDefined) {
      takenSet = ds.headOption.get match {
        case DataArray(label, arrayElems) => DataArray(label, arrayElems.take(numberOfItems))
        case DataRecord(label, elems) =>
          if (elems.forall(x => x.label == elems.head.label))
            DataArray(label, elems.take(numberOfItems))
          else
            DataArray("root", List(DataRecord(label, elems.take(numberOfItems))))
        case _ => DataArray(ds.label, ds.elems.take(numberOfItems).toList)
      }
    }
    DataRecord("take", List[DataSet]{takenSet})
  }



  def chunk(ds: DataSet, numberOfItemsPerChunk: Int): DataSet = {
    DataRecord("chunk", ds.elems.grouped(numberOfItemsPerChunk).map(p => DataArray("piece", p.toList )).toList)
  }

  def getDataSetWithHierarchy(ds: DataSet, hierarchyPath:Array[String]): List[DataSet] = {
    @tailrec
    var itemsToReturn = List[DataSet]()
    val nextItemInHierarchy = hierarchyPath.head
    val endOfHierarchyReached = if (hierarchyPath.tail.length == 0) true else false
    if (endOfHierarchyReached) {
      if(nextItemInHierarchy.equals("*")) {
        val l =  ds.elems.toList
        itemsToReturn = itemsToReturn:::l
      }
      else
        itemsToReturn = itemsToReturn:+ds(nextItemInHierarchy)
    }
    else {
      if(nextItemInHierarchy.equals("_"))
        itemsToReturn = itemsToReturn:::ds.elems.foldLeft(List[DataSet]()) { (z:List[DataSet], f:DataSet) => z:::getDataSetWithHierarchy(f, hierarchyPath.tail) }
      else
        itemsToReturn = itemsToReturn:::getDataSetWithHierarchy(ds(nextItemInHierarchy), hierarchyPath.tail)
    }
    itemsToReturn
  }

  /**
    * Flattens a hierarcy structure by copying items at the specified hierarchy level into the root
    * supports merging items at different hierarchy levels as "includes"
    *
    * @param ds   the dataset containing the hierarchy structure
    * @param args comma separated list of . notation hierarchy i.e. fieldA._.* will copy all the grandchildren of fieldA without knowing the child field names
    *             any hierarchy definitions prefixed with + will get appended to the copied items
    * @return     DataArray
    */
  def flattenStructure(ds: DataSet, args: List[String]): DataSet =
  {
    val fieldsToFlatten = args.filterNot(x => x.startsWith("+"))
    val fieldsToInclude = args.filter(x => x.startsWith("+")).map(x => x.substring(1))
    var flattenedList = List[DataSet]()
    for (field <- fieldsToFlatten) {
      val hierarchy = field.split('.')
      flattenedList = flattenedList:::getDataSetWithHierarchy(ds, hierarchy)
    }
    for ( field <- fieldsToInclude) {
      val hierarchy = field.split('.')
      val dataSetToInclude = getDataSetWithHierarchy(ds, hierarchy)
      flattenedList = flattenedList.map(x => DataRecord(x.label,x.elems.toList:::dataSetToInclude))
    }
    DataRecord("flatternedList", List[DataSet](DataArray("item",flattenedList)))
  }

  def dateFormat(ds: DataSet, format: String): DataSet = {
    try {
      DataString(new SimpleDateFormat(format).format(ds.asInstanceOf[DataDate].date))
    }
    catch {
      case _: Exception => DataString(new SimpleDateFormat(format).format(new SimpleDateFormat("dd/MM/yyyy").parse("1/1/1900")))
    }
  }

  def parseDate(dateStr: String, format: String): DataSet = {
    try {
      DataDate(new SimpleDateFormat(format).parse(dateStr))
    }
    catch {
      case e: Exception => throw new Exception(String.format("Unable to parseDate %s, with format %s",dateStr, format))
    }
  }

  def parseDateWithDefault(dateStr: String, format: String): DataSet = {
    try {
      DataDate(new SimpleDateFormat(format).parse(dateStr))
    }
    catch {
      case e: Exception => {
        DataString(new SimpleDateFormat(format).format(new Date))
      }
    }
  }

  def today(dateOffset: Int): DataSet = DataDate(DateUtils.addDays(new java.util.Date(), dateOffset))

  def convertDateFormat(ds: DataSet, in: String, out: String): DataSet = DataString(convDateValue(ds.stringOption.getOrElse(""), in, out))

  def ifNotBlankOrElse(ds: DataSet, other: String): DataSet = ds.stringOption.map(s => if (s.trim().isEmpty) DataString(other) else DataString(s)).getOrElse(DataString(other))

  def concatString(ds: DataSet): DataSet = DataString(ds.elems.filter(_.stringOption.isDefined).map(_.stringOption.get).mkString(","))

  //	  "{1-9}-{9-12}-{13-16}-{17-20}-{21-32}"
  def toUUIDFormat(str: String): DataSet = DataString(str.substring(0,8)+"-"+str.substring(8,12)+"-"+str.substring(12,16)+"-"+str.substring(16,20)+
    "-"+str.substring(20,32))

  def splitTrim(instr: String, delim: String): DataSet =  DataArray( instr.split(delim).toList.map( s => DataString(s.trim)  ))

  def nothing(): DataSet = DataNothing()

  def isBlank(ds: DataSet) = DataBoolean(ds.stringOption.exists(_.isEmpty))

  def isNull(ds: DataSet) = DataBoolean(ds.toOption.isEmpty)

  def size(ds: DataSet): DataSet = DataNumeric(ds.elems.size)

  def strContains(str: String, targetStr: String): DataSet = DataBoolean(if(str == null || targetStr == null) false else str.contains(targetStr))

  def substring(str: String, start: Int): DataSet = if (start < str.length) DataString(str.substring(start)) else DataString("")

  def substringWithEnd(str: String, start: Int, end: Int) = DataString(str.substring(start,if(str.length-1 < end) str.length-1 else end))

  def capitalise(str: String): DataSet = DataString(str.toUpperCase)

  def replaceAll(str: String, find: String, replaceWith: String): DataSet =
    try{ DataString(str.replaceAll(find, replaceWith))
    }
    catch {
      case _: Throwable => DataString(str)
    }

  def cleanStr(str: String) : DataSet =
    try {
      DataString(str.replace("[","_").replace("]","_").replace(".","_").replace("\n","").replace("\"","'"))
    }
    catch {
      case _: Throwable => DataString(str)
    }
  // single quote escape
  def sq(str: String): DataSet = if(str == null) DataString("") else DataString(str.replace("'","''"))

  def numeric(value: String): DataSet = DataNumeric(Try(BigDecimal(value)).getOrElse(BigDecimal(0)))

  def numericFormat(value: String, format: String) =  {

    //    if (value == "" && format == "#0")
    //     DataString("0")
    //   else
    DataString(new DecimalFormat(format).format(Try(BigDecimal(value)).getOrElse(BigDecimal(0))))
  }

  def sign(value: String): DataSet = DataNumeric(Try(BigDecimal(value).signum).getOrElse(0))

  def integer(value: String): DataSet = DataNumeric(Try(BigDecimal(value.toInt)).getOrElse(BigDecimal(0)))

  def round(value: String, scale: Int): DataSet = roundWithMode(value,scale,"HALF_UP")

  def roundWithMode(value: String, scale: Int, mode: String="HALF_UP"): DataSet = {
    val roundingMode =
      if (mode == None) BigDecimal.RoundingMode.HALF_UP
      else mode.toUpperCase() match {
        case "HALF_EVEN" =>
          BigDecimal.RoundingMode.HALF_EVEN
        case "HALF_DOWN" =>
          BigDecimal.RoundingMode.HALF_DOWN
        case "UP" =>
          BigDecimal.RoundingMode.UP
        case "DOWN" =>
          BigDecimal.RoundingMode.DOWN
        case "CEILING" =>
          BigDecimal.RoundingMode.CEILING
        case "FLOOR" =>
          BigDecimal.RoundingMode.FLOOR
        case _ =>
          BigDecimal.RoundingMode.HALF_UP
      }
    DataNumeric(Try(BigDecimal(value).setScale(scale, roundingMode)).getOrElse(BigDecimal(0)))
  }

  def csvWithHeader(ds: DataSet, delim: String): DataSet = {
    val csvSplit = delim + "(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)" // TODO: should allow encapsulation to be paramaterised
    val rows = ds.map(r => r(0).stringOption.getOrElse("").split(csvSplit, -1).map(c => c.replaceAll("^\"|\"$", ""))).toList
    if(rows.length == 0)
      DataNothing()
    else
      DataArray(rows.tail.map(r => DataRecord(rows.head.zipWithIndex.map(c => DataString(c._1, r(c._2))).toList)).toList)
  }

  def removeTrailingZeros(value: String): DataSet = DataNumeric(Try(BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP)).getOrElse(BigDecimal(0)).underlying().stripTrailingZeros())

  def batch(ds: DataSet): DataSet = DataRecord("", List(ds))

  def sumValues(ls: List[DataSet]) = DataNumeric(ls.foldLeft(BigDecimal(0))((d,l) => d + Try(BigDecimal(l.stringOption.getOrElse("0"))).getOrElse(BigDecimal(0))))

  def orElse(ds: DataSet, or: DataSet): DataSet = ds.toOption.getOrElse(or)

  def maprecord(ds: DataSet): DataSet = DataArray(ds.map(e => DataRecord(e)).toList)

  /* below will need to be replaced when I have time */

  def toJsonStr(ds: DataSet): DataSet = DataString("jsonstr", toJsonString(ds))

  def toJsonString(data: DataSet): String =
    data match {
      case DataString(_, s) => "\"" + s + "\""
      //  case DataString(l , s) => "\"" +l + "\" :  \"" + s + "\""
      case DataRecord(key, fs) =>
        toField(key) +
          "{" + fs.map(f =>
          (if (!f.isInstanceOf[DataRecord] && !f.isInstanceOf[DataArray]) toField(f.label) else "")
            + toJsonString(f)).mkString(",") +
          "}"
      case DataArray(key, ds) =>
        toField(key) +
          "[" + ds.map(d => "{"+ toJsonString(d) + "}").mkString(",") + "]"
      case DataNothing(_) => "null"
      case DataNumeric(_, num) => num.setScale(2, BigDecimal.RoundingMode.HALF_UP).underlying().stripTrailingZeros().toPlainString
      case DataBoolean(_, bool) => bool.toString
      case DataDate(_, date) => date.toString
      case e => toField(e.label) +
        "{" + e.map(toJsonString).mkString(",") + "}"
    }

  def toField(name: String): String = if (name.isEmpty) "" else "\"" + name + "\": "

  def convDateValue(value: String, in: String, out: String) =
    try {
      if(value.contains(":"))
        LocalDateTime.parse(value, DateTimeFormatter.ofPattern(in)).format(DateTimeFormatter.ofPattern(out))
      else
        LocalDate.parse(value, DateTimeFormatter.ofPattern(in)).format(DateTimeFormatter.ofPattern(out))
    }
    catch {
      case _: Exception => "1900-01-01 00:00:00.0"
    }

  def trimValue(value: String) = DataString(Option(value).getOrElse("").trim)

  def mapOrElse(v: String, colPairs: List[DataSet], orElse: String) = {
    val pairMap = colPairs.map(p => p.stringOption.getOrElse("")).grouped(2).map(g => (g.head, g.tail.headOption.getOrElse(""))).toMap
    DataString(pairMap.getOrElse(v, orElse))
  }

  def mapOrElseValue(value: String, colPairs: Map[String, String], orElse: String) = colPairs.getOrElse(value, orElse)

  def coalesce(vals: List[DataSet]) = vals.find(v => v.toOption.isDefined).getOrElse(DataNothing())

  def deDup(ds: DataSet, col: String): DataSet =
    DataArray(ds.elems.groupBy(e => e(col).stringOption.getOrElse("")).map(_._2.head).toList)

  def distinct(ds: DataSet, col: String) = {
    DataRecord(DataArray(ds.label, ds.elems.toList.groupBy((row: DataSet) => row(col)).map(_._2.head).toList))
  }

  // ================
  // CUSTOM FUNCTIONS
  //

  def subStringRegexp(instr: String, regexp: String) : String =
  {
    val rexp = regexp.r
    instr match {
      case rexp(x) => x
      case _ => ""
    }
  }

  def getSubStringRegexp(instr: String, regexp: String) : DataSet = {
    DataString(subStringRegexp(instr,regexp))
  }

  def getNumericRegexp(instr: String) : DataSet = {
    //  "(\d*\.?\d*)"

    DataString(subStringRegexp(instr,"""[^\+\-\d]*([\+\-\d]*\.?\d*).*"""))
  }


  // custom parse get the numeric part of a string
  def getNumericPrism(instr: String): DataSet = {
    val outstr  = subStringRegexp(instr,"""([\+-]?\d*\.?\d*).*""")
    DataString(outstr)
  }

  // custom parse get the numeric part of a string
  def getDirectionPrism(instr: String, checkStr: String, prismtype: String): DataSet = {
    // extract the direction In, Out, Up, Down from instr otherwise
    // convert True/False into horizontal(t=i,f=o) vertical(t=u,f=d)
    var outstr =
    subStringRegexp(instr.toUpperCase,"""^[ +-]*[\d]*\.?[\d \^]*[bB]?([uUdDiIoO]?).*""")

    if (outstr == "")
      if (prismtype == "H")
        if (checkStr == "T")
          outstr = "I"
        else
          outstr = "O"
      else if (prismtype == "V")
        if (checkStr == "T")
          outstr = "U"
        else
          outstr = "D"

    DataString(outstr)
  }

}
