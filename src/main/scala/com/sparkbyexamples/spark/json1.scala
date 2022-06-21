package com.sparkbyexamples.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}


object json1 {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("Jsonexample").master("local[*]").getOrCreate()

    val jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
    val data = Seq((1, jsonString))
    import spark.implicits._
    val df=data.toDF("id","value")
    df.show(false)

    val schema=new StructType()
                   .add("Zipcode",StringType,true)
                   .add("ZipCodeType",StringType,true)
                   .add("City",StringType,true)
                   .add("State",StringType,true)

    val fromjson=df.withColumn("value",from_json(col("value"),schema))
    fromjson.printSchema()
    fromjson.show(false)

    val df2=fromjson.select("id","value.*")
    df2.printSchema()
    df2.show(false)

     }

}
