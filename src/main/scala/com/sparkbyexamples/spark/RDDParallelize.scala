package com.sparkbyexamples.spark

import com.sparkbyexamples.spark.SparkContextExample.sqlCon.sparkContext
import org.apache.spark.sql.SparkSession

object RDDParallelize {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Parallelize").
      master("local[*]").getOrCreate()

    //val emptyRDD=spark.sparkContext.parallelize(Seq.empty[String])

    val rdd = spark.sparkContext.parallelize(List(1,2,3,4,5))
    val rddCollect = rdd.collect()
    println("Number of Partitions:"+rdd.getNumPartitions)
    println("Action: First element: "+rdd.first())
    println("Action: RDD converted to Array[Int] : ")
    rddCollect.foreach(println)




  }

}
