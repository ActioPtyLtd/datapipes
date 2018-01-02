
import actio.common.Data._
import actio.common.Dom
import org.scalatest.FunSuite

class DataSetFunctions extends FunSuite {

  val ds1 =
    DataRecord(
      DataString("str1","val1"),
      DataNothing("nothing"),
      DataRecord(
        DataString("str2", "val2")
      ),
      DataNumeric("num1", 10.5)
    )

  val ds2 =
    DataRecord(
      DataString("str1","val2"),
      DataNothing("nothing"),
      DataRecord(
        DataString("str2", "val2")
      ),
      DataNumeric("num2", 10.5)
    )


  test("if dot notation works from record") {
    val result = ds1("str1").stringOption

    assert(
      result.contains("val1")
    )
  }

  test("if selecting an empty fields string yields none") {
    val result = ds1("nothing").stringOption

    assert(
      result.isEmpty
    )
  }

  test("if flatten works") {
    val result = actio.common.Data.Operators.flatten(ds1)

    assert(
      result("str1").stringOption.contains("val1")
    )
    assert(
      result("str2").stringOption.contains("val2")
    )
  }

  test("if toString works") {
    assert(
      ds1("str1").toString == "val1"
    )
    assert(
      ds1("nothing").toString == ""
    )
    assert(
      ds1("record").toString == ""
    )
    assert(
      ds1("num1").toString == "10.5"
    )
  }

  test("if minus works") {
    assert(
      Operators.minus(ds1,ds1).isEmpty
    )
    assert(
      Operators.minus(ds1,ds2).contains(
        DataRecord(
          DataString("str1","val1"),
          DataNumeric("num1", 10.5)
        )
      )
    )
  }

}
