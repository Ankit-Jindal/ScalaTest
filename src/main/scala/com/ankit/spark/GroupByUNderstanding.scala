package com.ankit.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object GroupByUNderstanding {
  def main(args: Array[String]): Unit = {
    val filePath = "D:\\InputData\\spark_prabhu\\transaction.txt"

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("group by understanding")
      .getOrCreate()

    val schema = StructType(Array(StructField("TnID", IntegerType,false), StructField("Status",StringType, true), StructField("timestamp", StringType, true)))
    val inputDf = spark.read.
      format("csv").
      option("header","true").
      option("delimiter",",").
      schema(schema).
      load(filePath)

    val groupedByStatus = inputDf.
      groupBy(inputDf.col("status")).
      agg(count(lit(1)))
    groupedByStatus.explain()
    groupedByStatus.show()
  }
}
