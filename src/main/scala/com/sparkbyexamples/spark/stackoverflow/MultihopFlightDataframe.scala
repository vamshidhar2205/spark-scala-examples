package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}

object MultihopFlightDataframe {

  def main(args: Array[String]): Unit = {

  val spark=SparkSession.builder().appName("Multihopflights").master("local[*]").getOrCreate()

  val df1=spark.read.option("header","true").option("delimiter","|").option("inferSchema","true").
    csv("C:\\Users\\vamsh\\IdeaProjects\\spark-scala-examples\\src\\main\\resources\\txt\\flghts.txt")
    df1.printSchema()
    df1.show(false)

    val df2=spark.read.option("header","true").option("delimiter","|").option("inferSchema","true").
      csv("C:\\Users\\vamsh\\IdeaProjects\\spark-scala-examples\\src\\main\\resources\\txt\\flghts.txt")
    df2.printSchema()
    df1.show(false)


    val df3=df1.join(df2,df1("cid")===df2("cid") && df1("Destination")===df2("origin") &&
                      df1("cid")!=df2("cid"),"inner").select(df1("cid"),df1("origin"),df2("Destination")).show(false)




}
}
