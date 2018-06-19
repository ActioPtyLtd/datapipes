
import actio.common.Data.{DataNothing, DataString}
import actio.common.Dom
import actio.datapipes.task.TaskSetting
import org.scalatest.FunSuite

class DomTest extends FunSuite {

  val dom1 = Dom("dom1", Nil, DataString("str1","val1"), DataNothing(), Nil)
  val dom2 = Dom("dom2", Nil, DataString("str2","val2"), DataNothing(), Nil)
  val dom3 = Dom("dom3", Nil, DataString("str3", "val3"), DataNothing(), Nil)

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
      new actio.datapipes.task.Term.TermExecutor(TaskSetting("","",false)).eval(result, "dom => dom.dom2.str2").stringOption.contains("val2")
    )
    assert(
      new actio.datapipes.task.Term.TermExecutor(TaskSetting("","",false)).eval(result, "dom => dom.dom3.str3").stringOption.contains("val3")
    )
  }

}
