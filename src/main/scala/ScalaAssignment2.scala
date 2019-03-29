

object ScalaAssignment2 {

  def main(args: Array[String]): Unit = {
    // test factorial using loop
    println(factorialWithLoop(5))

// no of words in a string
    val str = "This line has 5 words"
    println(noOfWords(str))
    println(noOfWords(null))
println("*********************Array Test ********************************")
    arrayTes

    println("*********************Array To List ********************************")
    arrayToList

    println("********************* List ********************************")
    listBasic()
  }
  // Scala function to calculate factorial using loop
  def factorialWithLoop(number : Int) : Long =
  {
    var factorial : Long = 1;
    for( i <- (1 to number)) {
      factorial = factorial * i
    }
    factorial
  }

  // number of words in a string using loop
  def noOfWords(str : String) : Long = {
    var noOfwords :Long = 0;
    if(str != null && !str.isEmpty){
      noOfwords = str.split(" ").length
    }
    noOfwords
  }

  // define initialise and display each value by using a loop
  def arrayTes(): Unit ={
    val array : Array[Int] = new Array(10)

    println(manOf(array))
    println(" Values of array initially")
    for( elem <- array){
      print(elem + " ")
    }
    println(" Print complete for Values of array initially")
    println(" Inserting values to array")
    for(i <- 0 to 9) {
      array(i) = i+2
    }

    for( elem <- array){
      print(elem + " ")
    }
  }

  // define array , add some values and convert this array to list

  def arrayToList: Unit = {
    val array : Array[String] = Array("My", "name"," is"," Scala")
    val list = array.toList
    println(list.getClass)
    println(manOf(list))
  }

  // Insert 10 values to List and find min and max
  def listBasic(): Unit ={
    var list : List[Int] = List()
    list = list.+:(2)
    for(i <- list) {
      print(i+" ")
    }
  }

  def manOf[T:Manifest](t:T): Manifest[T] = manifest[T]
}
