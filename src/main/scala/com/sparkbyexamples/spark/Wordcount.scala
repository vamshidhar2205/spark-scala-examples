package com.sparkbyexamples.spark
import org.apache.spark.sql.SparkSession

object Wordcount extends App {

  val spark= SparkSession.builder().appName("Wordcount").master("local[*]").getOrCreate()

  val rdd1=spark.sparkContext.textFile("C:\\Users\\vamsh\\IdeaProjects\\spark-scala-examples\\src\\main\\resources\\test.txt")

  rdd1.foreach(println)

  val rdd2=rdd1.flatMap(f=>f.split(" "))

  val rdd3=rdd2.map(x=>(x,1))

  //rdd3.foreach(println)

  //Prints key value pairs that starts with a
  val rdd4 = rdd3.filter(a=> a._1.startsWith("a"))
  rdd4.foreach(println)

  //Reduce By Key
  val rdd5 = rdd3.reduceByKey(_ + _)
  rdd5.foreach(println)

  //Prints reduced by key value pairs that starts with a
  val rdd6 = rdd5.filter(a=> a._1.startsWith("a"))
  rdd6.foreach(println)

  //Swap word,count and sortByKey transformation
  val rdd7 = rdd5.map(a=>(a._2,a._1)).sortByKey()
  rdd7.foreach(println)

  //Action - Count
  println("Count : "+rdd7.count())

  //Action - first
  println("Count : "+rdd7.first())

  //Action - max
  val Maxelement = rdd7.max()
  println(Maxelement)
  println("Max Record : "+Maxelement._1 + ","+ Maxelement._2)

  //Action - reduce
  val totalWordCount = rdd7.reduce((a,b) => (a._1+b._1,a._2))
  println("dataReduce Record : "+totalWordCount._1)

  //Action - take
  val take3 = rdd7.take(3)
  take3.foreach(f=>{
    println("Key:"+ f._1 +", Value:"+f._2)})
}
