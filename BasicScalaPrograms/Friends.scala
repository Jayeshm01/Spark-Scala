package com.jayesh.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Friends {
  
  def getData(line: String) = {
    val fields = line.split(",")
    val name = fields(1).toString
    val numFriends = fields(3).toInt
    (name,numFriends)
  }
  
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","Freinds")
    val lines = sc.textFile("../fakefriends.csv")
    val rdd = lines.map(getData)
    val totalByName = rdd.mapValues(x => (x,1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    val avgByName = totalByName.mapValues(x => x._1 / x._2)
    val results  = avgByName.map(x => (x._2 ,x._1)).sortByKey().collect() 
    results.foreach(println)
    
  }
  
  
}