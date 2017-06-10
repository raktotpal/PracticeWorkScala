package com.internal.spark.test

import java.util.Date
import java.text.SimpleDateFormat

object ScalaTest {
  var myVar: String = "Foo"
  val (myVar1: Int, myVar2: String) = Pair(40, "Foo")

  // val or val VariableName : DataType [=  Initial Value]
  def main(args: Array[String]) {

    val timeGMT = 1435689100078L;

    val formatTime = "yyyy/MM";

    val sdf = new SimpleDateFormat(formatTime)

    println(sdf.format(timeGMT))

    //    println("Hello\tWorld\n\n " + myVar);
    //
    //    // Call Function with Named ARGS
    //    printInt(b = 5, a = 7);
    //
    //    // function with variable parameters
    //    printStrings("Hello", "Scala", "Python");
    //
    //    // If in function call, parameters are not passed, default values are assigned in calling function.
    //    println("Returned Value : " + addInt2());
    //
    //    
    //    // Function having parameters as functions
    //    println(apply(layout, 10))
    //    
    //    
    //    
    //    // first-class function
    //    var mul = (x: Int, y: Int) => x*y
    //    
    //    println(mul(4, 6))
    //    
    //    
    //    
    //    // partially applied function
    //    /*
    //     * As the function takes first argument everytime; only differing the 2nd arg in each call,
    //     * we partially apply this function by binding with only 1st func.
    //     * 
    //     */
    //    val date = new Date
    //      val logWithDateBound = log(date, _ : String)
    //
    //      logWithDateBound("message1" )
    //      logWithDateBound("message2" )
    //      logWithDateBound("message3" )
  }

  def addInt(a: Int, b: Int): Int = {
    var sum: Int = 0
    sum = a + b

    return sum
  }

  // Return Type 'Unit' is equivalent to VOID in java.
  def printMe(): Unit = {
    println("Hello, Scala!")
  }

  def printInt(a: Int, b: Int) = {
    println("Value of a : " + a);
    println("Value of b : " + b);
  }

  def printStrings(args: String*) = {
    var i: Int = 0;

    //********************* FOR-EACH LOOP
    for (arg <- args) {
      println("Arg value[" + i + "] = " + arg);
      i = i + 1;
    }
  }

  /*
    * If parameters are not passed, default values are assigned in calling function.
    */
  def addInt2(a: Int = 5, b: Int = 7): Int = {
    var sum: Int = 0
    sum = a + b

    return sum
  }

  def apply(f: Int => String, v: Int) = f(v)

  def layout[A](x: A) = "[" + x.toString() + "]"

  def log(date: Date, message: String) = {
    println(date + "----" + message)
  }

}