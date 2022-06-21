package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel

object CacheAndPersist extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark:SparkSession = SparkSession.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate()
  import spark.implicits._
  val columns = Seq("Seqno","Quote")
  val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."))
  //Different ways to define DF columns
  //val df=data.toDF()
 //val df = data.toDF("Seqno.","Quote")

  val df=data.toDF(columns: _*)

  val dfCache = df.cache()
  dfCache.show(false)

  val dfPersist=df.persist()
  dfPersist.show(false)

  val dfPersist1=df.persist(StorageLevel.MEMORY_ONLY)
  dfPersist1.show(false)

  val dfUnpersist=dfPersist1.unpersist()
  dfUnpersist.show(false)

 //val values = List(List("1", "One") ,List("2", "Two") ,List("3", "Three"),List("4","4")).map(x =>(x(0), x(1)))
 //values.foreach(println)
 //val df1 = values.toDF()
  //df1.show(false)


}
