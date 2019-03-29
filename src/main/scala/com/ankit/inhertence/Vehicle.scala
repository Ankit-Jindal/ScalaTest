package com.ankit.inhertence

class Vehicle(val id:Int, val year:String) {
  override def toString: String = s"$id , $year"
}
