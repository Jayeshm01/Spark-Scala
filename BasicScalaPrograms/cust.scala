package com.jayesh.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object cust {
  
  def parseData(line: String) = {
    val fields = line.split(",")
    val CustId = fields(0).toInt
    val amtSpent = fields(2).toFloat
    (CustId,amtSpent)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","cust")
    val custData = sc.textFile("../customer-orders.csv")
    val rdd = custData.map(parseData)
    val amountByCust = rdd.reduceByKey((x,y) => x + y)
    val results = amountByCust.map(x => (x._2, x._1)).sortByKey().collect()
    
    for (result <- results){
      val CustId = result._2
      val Amount = result._1
      println(s"$CustId,$Amount")
    }
  }
  
}