package com.ankit.sparkbasic

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object existingHiveTable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Hive Tables").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)
    sqlC.sql("show databases").show()
    val df = sqlC.sql("select * from employees")
    df.write.mode(SaveMode.Overwrite)
  }
}
