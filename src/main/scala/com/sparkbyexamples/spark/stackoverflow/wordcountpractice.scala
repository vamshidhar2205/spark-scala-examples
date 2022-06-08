package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.sql.SparkSession

object wordcountpractice {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]").appName("wordcount").getOrCreate()

    val rdd=spark.sparkContext.textFile("C:\\Users\\vamsh\\IdeaProjects\\spark-scala-examples\\src\\main\\resources\\csv\\sample.csv")

    rdd.foreach(println)

    //Wordcount using ReducebyKey
    val rdd1=rdd.flatMap(line=>line.split(" "))
    val rdd2=rdd1.map(word=>(word,1))
    val rdd3=rdd2.reduceByKey((a,b)=>(a+b))
    rdd3.foreach(println)

    //Print values that stars with a and also ReducedByKey values that starts with a

    val startswithA=rdd2.filter(x=>x._1.startsWith("a"))
    startswithA.foreach(println)

    val startswithAreduce=rdd3.filter(y=>y._1.startsWith("a"))
    startswithAreduce.foreach(println)

    val usinggroupby=rdd3.groupByKey()
    usinggroupby.foreach(println)

    val sort=rdd3.map(s=>(s._2,s._1)).sortByKey()
    sort.foreach(println)

    println("Count : "+sort.count())

    val totalwordcount=rdd3.reduce((a,b)=>(a._1+b._1,a._2))
    println("datareduce:" + totalwordcount._1)

    val take3=sort.take(3)
    take3.foreach(f=>{println("Key:"+ f._1,"Value:"+ f._2)})
  }
}
