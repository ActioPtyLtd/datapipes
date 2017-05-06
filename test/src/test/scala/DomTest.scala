import Common.Data.{DataNothing, DataString}
import Common.Dom
import org.scalatest.FunSuite

class DomTest extends FunSuite {

  val dom1 = Dom("dom1",null, Nil, DataString("str1","val1"), DataNothing())
  val dom2 = Dom("dom2",null, Nil, DataString("str2","val2"), DataNothing())
  val dom3 = Dom("dom3",null, Nil, DataString("str3", "val3"), DataNothing())

  test("Dom merge and select by name") {
    val result = Dom() ~ dom1 ~ dom2

    assert(
      result("dom1") == dom1
    )
    assert(
      result("dom2") == dom2
    )
  }

  test("Dom merge and select head") {
    val result = Dom() ~ dom1 ~ dom2 ~ dom3

    assert(
      result.headOption.contains(dom3)
    )
  }

  test("Dom interpret select") {
    val result = Dom() ~ dom1 ~ dom2 ~ dom3

    assert(
      Term.TermExecutor.eval(result, "dom => dom.dom2.str2").stringOption.contains("val2")
    )
    assert(
      Term.TermExecutor.eval(result, "dom => dom.dom3.str3").stringOption.contains("val3")
    )
  }

}