object ScalaTest {

  def main(args: Array[String]): Unit = {

    var str1 = "Str1"
    var str2 = "Str2"

    val res: Boolean = str2.==(str1)

    print(res)
  }

  def methodParam(a: String) :Unit = {
    a = "reassgning"
    println(a)
  }
}
