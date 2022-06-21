package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object ToJSONandFromJSON {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("toandfromjson").master("local[*]").getOrCreate()

    import org.apache.spark.sql.types._

    import spark.implicits._

    val jsonschema=new StructType()
      .add("eid",IntegerType,true)
      .add("ename",StringType,true)
      .add("dept",StringType,true)
      .add("salary",IntegerType,true)

    val columnnames=Array("eid","ename","dept","salary")

    val df1=spark.sparkContext.parallelize(Seq(
                                        (101,"Apple","Social",10000),
                                        (102,"Orange","Science",20000),
                                        (103,"Banana","Math",30000),
                                        (104,"Mango","History",40000)))
    val df2=df1.toDF(columnnames: _*)
    df2.printSchema()
    df2.show(false)

    val df3=df2.toJSON
    df3.show(false)

    val df4=df3.withColumn("record",from_json(col("value"),jsonschema))
    df4.printSchema()
    df4.show(false)

    val df5=df4.select("record.*").drop("value")
    df5.show(false)

  }

}
