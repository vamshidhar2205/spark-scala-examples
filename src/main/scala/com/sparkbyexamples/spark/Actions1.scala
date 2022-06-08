package com.sparkbyexamples.spark

import org.apache.spark.sql.SparkSession

object Actions1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Actions1").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val inputrdd = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))
    val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))
    inputrdd.foreach(println)
  }

}
