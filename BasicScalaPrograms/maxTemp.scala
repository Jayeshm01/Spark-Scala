package com.jayesh.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

object maxTemp {
  def parseData(line: String) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temp = fields(3).toFloat * 0.1f * (9.0f/5.0f) + 32.0f
    (stationId,entryType,temp)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","maxTemp")
    val lines = sc.textFile("../1800.csv")
    val rdd = lines.map(parseData)
    val maxTemps = rdd.filter(x => x._2 == "TMAX")
    val maxStationTemp = maxTemps.map(x => (x._1,x._3.toFloat))
    val maxYearTemp = maxStationTemp.reduceByKey( (x,y) => max(x,y))
    val results = maxYearTemp.collect()
    
     for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station maximum temperature: $formattedTemp") 
    }
    
  }
}