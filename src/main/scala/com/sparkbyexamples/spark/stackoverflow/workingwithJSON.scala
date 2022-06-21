package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object workingwithJSON {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("Jsonexample").master("local[*]").getOrCreate()

    val jsondF=spark.read.option("multiLine",true).json("C:\\Users\\vamsh\\IdeaProjects\\spark-scala-examples\\src\\main\\resources\\zipcodes_streaming\\jsonpractice.json")
    jsondF.printSchema()
    jsondF.show(false)

    //Rename id to key and print schema
    val df1=jsondF.withColumnRenamed("id","key")
    df1.printSchema()

    //select coulmns key and batter from the json file
    val batter1=df1.select(col("key"),col("batters.batter"))
    batter1.printSchema()
    batter1.show(false)

    //Use explode to convert rows to columns for the array "batter"
    val batter2=batter1.select(col("key"),explode(col("batter")).alias("new_batter"))
    batter2.printSchema()
    batter2.show(5,false)

    //Using . and * to extract the values of struct type
    val batter3=batter2.select(col("key"),col("new_batter.*")).
                withColumnRenamed("id","bat_id").
                withColumnRenamed("type","bat_type")
    batter3.printSchema()
    batter3.show(5,false)

    //Combining all the fucntions above for the topping array of struct
    val topping1=df1.select(col("key"),col("topping"))
    val topping2=topping1.select(col("key"),explode(col("topping")).alias("new_topping")).
      select(col("key"),col("new_topping.*")).
      withColumnRenamed("id","top_id").withColumnRenamed("type","top_type")
    topping2.printSchema()
    topping2.show(5,false)

  }

}
