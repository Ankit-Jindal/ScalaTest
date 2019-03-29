package com.ankit.com.ankit.sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("FirstStreamingApp").set("spark.hadoop.fs.defaultFS","hdfs://192.168.56.102:8020")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))

    val dstream = ssc.socketTextStream("192.168.56.102",9999)
    dstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
   // nc -lk 9999
    ssc.start()
    ssc.awaitTermination()
  }
}
