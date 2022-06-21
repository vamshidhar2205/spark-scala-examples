package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object RepartitionAndCoalesce {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark=SparkSession.builder().appName("RepAndCoa").master("local[*]").getOrCreate()

    val rdd=spark.sparkContext.parallelize(Range(0,15))
    println("No. of partitions:"+rdd.partitions.size)

    val rdd1=spark.sparkContext.parallelize(Range(0,20),6)
    println("No. of partitions:"+rdd1.partitions.size)

    val rdd2=rdd1.repartition(4)
    println("No. of partitions:"+rdd2.partitions.size)

    val rdd3=rdd2.coalesce(2)
    println("No.of partitions:"+rdd3.partitions.size)

    //Increasing partitions using coalesce results in max partition of previous rdd
    //val rdd3=rdd2.coalesce(6)
    //println("No.of partitions:"+rdd3.partitions.size)

  }

}
