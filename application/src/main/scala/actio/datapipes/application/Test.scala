package actio.datapipes.application

import com.typesafe.config.{Config, ConfigFactory}

object Test {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseString(
      """
         other {
           stuff = ""
         }
test {

  name = "asdf"
  hello = "hello "${this.name}
}
      """

    )




    config.resolve()

    Console.println(config)

  }

  def myresolve(keys: List[String]) = {

  }
}
