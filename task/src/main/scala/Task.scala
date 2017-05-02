import Common.Task

object Task {
  def apply(s: String): Task = new TaskExtract()
}
