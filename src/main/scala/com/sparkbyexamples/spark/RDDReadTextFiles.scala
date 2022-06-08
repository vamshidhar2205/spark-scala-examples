package com.sparkbyexamples.spark

import com.sparkbyexamples.spark.rdd.ReadTextFiles.spark
import org.apache.spark.sql.SparkSession

object RDDReadTextFiles extends App {

  val spark = SparkSession.builder().appName("ReadextFiles").
    master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val rdd = spark.sparkContext.textFile("C:\\Users\\vamsh\\IdeaProjects\\spark-scala-examples\\src\\main\\resources\\csv\\text01.txt")
 rdd.foreach(println)
  val rdd1 = spark.sparkContext.textFile("C:\\Users\\vamsh\\IdeaProjects\\spark-scala-examples\\src\\main\\resources\\csv\\*")
 rdd1.foreach(println)
  val rdd2 = spark.sparkContext.wholeTextFiles("C:\\Users\\vamsh\\IdeaProjects\\spark-scala-examples\\src\\main\\resources\\csv\\*")
  rdd2.foreach(println)

  val rdd3 = rdd.map(f=>{
    f.split(",")
  })

  rdd3.foreach(f => {
    println("Col1:"+f(0)+",Col2:"+f(1))
  })

  val rdd4 = spark.sparkContext.parallelize(Seq(("Java",2000),("Python", 100000), ("Scala", 3000)))
  rdd4.foreach(println)

  val rdd5 = rdd4.map(row=>{(row._1,row._2+100)})
}
