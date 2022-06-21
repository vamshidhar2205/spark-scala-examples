package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.sql.SparkSession

import org.apache.log4j.{Level, Logger}

object shufflepartitions {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark=SparkSession.builder().appName("Shufflepartitions").master("local[*]").getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions",100)

    val rdd=spark.sparkContext.textFile("C:\\Users\\vamsh\\IdeaProjects\\spark-scala-examples\\src\\main\\resources\\txt\\holmes.txt")

    println("RDD Parition Count :"+rdd.getNumPartitions)
    val rdd1=rdd.flatMap(f=>f.split(" ")).map(x=>(x,1)).reduceByKey(_+_)

    rdd1.foreach(println)

    println("RDD Parition Count :"+rdd1.getNumPartitions)

    import spark.implicits._

    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )

    val df=simpleData.toDF("employee_name","department","state","salary","age","bonus")

    val df2=df.groupBy("state").count()
    println("No. of partitions:" + df2.rdd.getNumPartitions)
  }



}
