package com.ankit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.Row
object MostSoldPairs {
  def main(args: Array[String]): Unit = {
 val spark = SparkSession.builder().master("local[*]").appName("Pair100").getOrCreate()
   /* val orderPath = "src/main/resources/Order.csv"
    val productPath = "src/main/resources/product.csv"*/

    val orderPath = "C:\\Users\\Ankit Jindal\\IdeaProjects\\ScalaTest\\src\\main\\resources\\Order.csv"
    val productPath = "C:\\Users\\Ankit Jindal\\IdeaProjects\\ScalaTest\\src\\main\\resources\\product.csv"

    val df = spark.read.format("csv").option("header", true).option("inferSchema","true").load(orderPath)

/*
    val inputOrderRawRdd = spark.sparkContext.textFile(orderPath)
    val inputOrderRdd = inputOrderRawRdd.map(rec => {
      val arr = rec.split(",")
      (arr(0).toInt,arr(1).toInt)
    })
*/
    val inputOrderRdd = df.rdd.map(rec => (rec(0).asInstanceOf[Int], rec(1).asInstanceOf[Int]))
    val grpByUserId = inputOrderRdd.groupByKey()
    val userWithPairProd = grpByUserId.flatMapValues( itr => itr.toList.sorted.combinations(2).map{case Seq(x,y) => (x,y) }.toList)
    val pairProductSold = userWithPairProd.map(rec => (rec._2,1))

    val pairsProdByPopularity = pairProductSold.reduceByKey((sum, v) => sum+v).map(rec => Row(rec._1._1,rec._1._2, rec._2))
     // .sortBy(rec => rec._2, false)

    val manualSchema = StructType(Array(StructField("Prod1",IntegerType, false),StructField("Prod2",IntegerType, false),StructField("UserCount",IntegerType, false)))
   val pairOrderedDf =  spark.createDataFrame(pairsProdByPopularity,manualSchema)

    pairOrderedDf.createOrReplaceGlobalTempView("ProdPairOrder")
/*    val top100 = pairsProdByPopularity.take(100).map(rec => (rec._1._1, rec._1._2, rec._2)*/

pairOrderedDf.show()
    val productDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(productPath)

    productDF.createOrReplaceGlobalTempView("Product")
    spark.sql(
      """ select ppo.Prod1, ppo.Prod2,
           p1.prod_name, p2.prod_name,
         ppo.UserCount
        from (select * from global_temp.ProdPairOrder order by UserCount desc limit 100) ppo
        join global_temp.Product p1
        on ppo.Prod1 = p1.prod_id
        join global_temp.Product p2 on p2.prod_id = ppo.Prod2 """).show()
//     spark.sql(
//      """ select
//          ppo.Prod1, ppo.Prod2,
//          case when ppo.Prod1 = p.prod_id then p.prod_name end p1,
//        case when ppo.Prod2 = p.prod_id then p.prod_name end p2,
//         ppo.UserCount, p.*
//        from (select * from global_temp.ProdPairOrder order by UserCount desc limit 2) ppo join global_temp.Product p
//        on (ppo.Prod1 = p.prod_id or ppo.Prod2 = p.prod_id) """).show()

  //  pairWithNames.createOrReplaceGlobalTempView("Prod")


  }
}
