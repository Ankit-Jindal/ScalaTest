package com.ankit.com.ankit.sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FirstStreamingProg {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("FirstStreamingApp").set("spark.hadoop.fs.defaultFS","hdfs://192.168.56.102:8020")
     val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val dirPath = "/user/cloudera/sparkrepartiion/*"

    val rdd = sc.textFile(dirPath)
    rdd.count()
    val dstream : DStream[String] = ssc.textFileStream(dirPath)

    val wordsDStream = dstream.flatMap(_.split(" "))

    val countStream = wordsDStream.map((_,1)).reduceByKey(_+_)

    countStream.print()
    countStream.foreachRDD(rdd => rdd.foreach(println))

    ssc.start()
    ssc.awaitTermination()
  }
}
