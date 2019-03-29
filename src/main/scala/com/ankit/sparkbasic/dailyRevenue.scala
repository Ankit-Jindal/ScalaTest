package com.ankit.sparkbasic

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object dailyRevenue {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Daily Revenue").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)
    val fileOrders = "hdfs://quickstart.cloudera:8020/user/cloudera/retail_db/orders"

    val orderRdd = sc.textFile(fileOrders)
    val tupledRdd = orderRdd.map(
                    rec => {val arr = rec.split(",")
                      (arr(0).toInt,arr(1),arr(2).toInt,arr(3))
                    })

    import sqlC.implicits._
    val orderDF = orderRdd.toDF()

    orderDF.printSchema()

    orderDF.show(10)


  }
}
