object ScalaTest {

  def main(args: Array[String]): Unit = {

/*    var str1 = "Str1"
    var str2 = "Str2"

    val res: Boolean = str2.==(str1)

    print(res)

    var methodParam = new MethodParam
    methodParam.age = 20
    println(methodParam.age)

    usemethodParam(methodParam)
    println(methodParam.age)
    */
   //val sum1 =  sum(1000000000) // will generate stack overflow error
   // println("Sum is " + sum1)

    println("Sum with tail recursion " + sumWithTailRec(1000000100,0))
    println("Sum with tail recursion " + sumWithTailRec(3,0))
  }

/*  def methodParam(a: String) :Unit = {
    a = "reassgning"
    println(a)
  }*/

  def usemethodParam(a : MethodParam) = {
    a.age = 30
  }
  // Sum method with recursion.
  // Sum of  0 to n with recursion
  // In the below function there is a possibility of getting stack over flow as stack has a variable and a function call.
  def sum(i :Int) :Int ={
    if(i == 0) 0
    else i + sum(i - 1)
  }

  // below method does uses recursion but tail recursion.
  // BY which each stack frame can be replaced by other as stack frame does not hold any variable.

  def sumWithTailRec(n :Int, sum: Long) :Long ={
  if( n == 0) sum
  else {
  sumWithTailRec(n - 1, sum + n )
  }

  }

}
