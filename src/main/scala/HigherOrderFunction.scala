object HigherOrderFunction {

  def main(args: Array[String]): Unit = {

    // sum of sqaures
    println("Sum of Squares " + sumOfSquare(1,3))
    println("Sum of Squares " + sumOfAnyFunction(square,1,3))
    println("Sum of Squares " + sumOfAnyFunction(squareLiteral,1,3))
  }

  def identity(i: Int): Int = i

  def square(i: Int): Int = i * i

  val squareLiteral = (a:Int) => a*a

  def sumOfSquare(a: Int, b: Int): Int = {
    if (a == b) square(a)
    else {
      square(a) + sumOfSquare(a+1,b)
    }

  }

  def sumOfAnyFunction( f: Int => Int, a:Int, b:Int) :Int = {
    if( a== b) f(a)
    else {
      f(a) + sumOfAnyFunction(f , a+1,b)
    }
  }
}
