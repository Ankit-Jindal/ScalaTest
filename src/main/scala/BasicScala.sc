val i:Int = 10
// i =12   --- Reassignment to val (Compilation error)

var hello1 : String = "Hello"
var  hello2 = null

hello2 == hello1

def sum(a:Int, b:Int):Int = {
  a+b
}

sum(10,20)

// Function Literal

(a:Int, b:Int) => {a+b}:Int

val sumVar = (a:Int, b:Int) => {a+b}

var sumVar2 : (Int,Int) => Int = (a:Int, b:Int) => {a+b}
sumVar(2,3)