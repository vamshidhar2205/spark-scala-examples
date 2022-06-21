package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.sql.SparkSession

object RDDActionsPractice {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("RDDActions").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputRDD = spark.sparkContext.parallelize(List(("Z", 1), ("A", 20), ("B", 30), ("C", 40), ("B", 30), ("B", 60)))
    val listRDD = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 3, 2))

    //Collect
    val data = listRDD.collect()
    data.foreach(println)
    println("Number of Partitions:"+listRDD.getNumPartitions)
    val listRDD1=listRDD.repartition(7)
    println("Number of Partitions:"+listRDD1.getNumPartitions)

    //Took * as aggregate value instead of + and zero value as 2
    def param0= (accu: Int, v: Int) => accu * v

    def param1 = (accu1: Int, accu2: Int) => accu1 * accu2
    println("aggregate:" + listRDD1.aggregate(2)(param0, param1))

    def param2= (accu: Int, v:(String,Int)) => accu + v._2
    def param3 = (accu1: Int, accu2: Int) => accu1 + accu2
    println("aggregate:" + inputRDD.aggregate(0)(param2, param3))

    //fold
    println("fold :"+listRDD.fold(0)
    {(acc,v)=>
      val sum=acc+v
      sum
    })
    println("fold :"+inputRDD.fold(("Total",0))
      {(acc:(String,Int),v:(String,Int))=>
        val sum=acc._2+v._2
        ("Total",sum)
      })

    //reduce
    println("reduce :"+listRDD1.reduce(_+_))
    println("reduce alternate :"+ listRDD.reduce((x,y)=>(x+y)) )
    println("reduce :"+inputRDD.reduce((x,y)=>("Total",x._2+y._2)))
  }
}
