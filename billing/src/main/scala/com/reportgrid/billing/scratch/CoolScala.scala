package com.reportgrid.billing.scratch

object CoolScala {
  def main(args: Array[String]) {
    val r = foo {
      results
    }
    
    println(r)
  }
  
  def results(): String = {
    println("during")
    Thread.sleep(1000)
    "results"
  }
  
  def foo[T](b: => T): T = {
    println("before")
    val r = b
    println("after")
    r
  }
}