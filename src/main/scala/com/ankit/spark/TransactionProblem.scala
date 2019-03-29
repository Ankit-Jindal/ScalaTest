package com.ankit.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object TransactionProblem {
//get average time per transaction.
  // count of transaction finished in 0 sec, 1, 2, 3, 4, 5, sec

  def main(args: Array[String]): Unit = {
    val filePath = "D:\\InputData\\spark_prabhu\\transaction.txt"
    transactionProblemWithDf(filePath)
  }

  def transactionProblemWithDf(filePath : String):Unit = {

    val spark = SparkSession.builder().appName("Tranasation Problem").master("local[*]").getOrCreate()

    import spark.implicits._
    import spark.sqlContext.implicits._
    import spark.sqlContext.udf
    val df = spark.read.format("csv").option("header","true").load(filePath)

   // val df =  df1.cache()
    val trnformedDF = df.select(col("TnID"),col("STATUS"),unix_timestamp(col("TIMESTAMP"), "yyyy/MM/dd hh:mm:ss:SSS").as("time"))
   // trnformedDF.cache()
    val pendingDF = trnformedDF.filter(col("STATUS") === "050").as("pending")
    val sucessDF = trnformedDF.filter(trnformedDF.col("STATUS") === "000").as("success")

    val trnJoined = pendingDF.join(sucessDF, sucessDF.col("TnID") === pendingDF.col("TnID"), "inner")

    val toCalcDiff = trnJoined.select(col("success.TnID"), abs(col("success.time").-(col("pending.time"))).as("timeTaken"))

// select * from abc where id in ()
    //trnformedDF.where(col("TnID").isin())
    //toCalcDiff.cache()
    toCalcDiff.explain()
    //
    val avgTime = toCalcDiff.select(avg(toCalcDiff.col("timeTaken")).as("AvgTime"))

    val avgTimeForAllTransactons: Double = avgTime.first.getAs[Double](0)

    print(avgTimeForAllTransactons)


      }

  def transactionProb():Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Transaction Calculation").setMaster("local[*]"))
    val sqlC = new SQLContext(sc)
    val file = "D:\\InputData\\spark_prabhu\\transaction.txt"
    val rddFile = sc.textFile(file)
    val skippedHeader = rddFile.mapPartitionsWithIndex((idx, itr) => if(idx == 0 && itr.hasNext){itr.next; itr} else itr)
    val rddSplit = skippedHeader.map(line => {
      def getTimeMillSec(strDate : String) : Long = {
        val sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss:SSS")
        val calander = Calendar.getInstance()
        val date = sdf.parse(strDate)
        calander.setTime(date)
        calander.getTimeInMillis
      }
      val arr = line.split(",")
      (arr(0), arr(1),getTimeMillSec(arr(2)))
    })

    val pairRdd = rddSplit.map(tup => (tup._1,(tup._2,tup._3)))
    val groupedRdd = pairRdd.groupByKey()
    val filteredGroupedRdd = groupedRdd.filter(tup => tup._2.size == 2 )
    val tnWithTimeTaken = filteredGroupedRdd.map(rec => {
      val list = rec._2.toList
      val diff = math.abs(list(0)._2 - list(1)._2)/ 1000 // sec
      (rec._1, diff)
    })

    tnWithTimeTaken.cache()

    val sumCount = tnWithTimeTaken.aggregate((0l,0))((total, value)=> (total._1 + value._2, total._2 + 1),(total1,total2)=> (total1._1 + total2._1, total1._2 + total2._2))
    val averageTime = sumCount._1.toDouble/sumCount._2
    println(averageTime)
    /*    val averageRddTo= tnWithTimeTaken.aggregateByKey((0l,0))((total, value)=> (total._1+value, total._2 + 1),(aggTotal, total)=> (aggTotal._1 + total._1, aggTotal._2 + total._2))
        val averageTimePerTn = averageRddTo.map(rec => (rec._1, rec._2._1/rec._2._2 ))*/

    // finding number of records in 0,1 ,2, 3 , 4,5 sec

    val timeTrnRdd = tnWithTimeTaken.map(rec => (rec._2, 1))

    val timeGrouped = timeTrnRdd.reduceByKey(_+_).sortBy(rec => rec._1)

    timeGrouped.collect().foreach(println)
  }


}
