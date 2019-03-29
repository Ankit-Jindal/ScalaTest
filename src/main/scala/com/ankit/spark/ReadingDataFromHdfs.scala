package com.ankit.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadingDataFromHdfs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read HDFS")
      .master("yarn")
      .config("spark.hadoop.fs.defaultFS","hdfs://192.168.56.101:8020")
      .config("spark.hadoop.resourcemanager.address","quickstart.cloudera:8032")
      .getOrCreate()

    println("**********Spark in yen mode**********")

    val df = spark.read.format("csv").option("inferSchema" ,"true").option("header","true").load("/user/cloudera/flight_data/flight_2015.csv")

    df.write.mode(SaveMode.Ignore)
  }
}
