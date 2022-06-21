package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.sql.functions.{avg, max, min, sum}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}

object PivotExample {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("Pivot").master("local[*]").getOrCreate()

    val data= Seq(Row("Stock","Plus500",0.0592,"Amazon","01/11/2021",3368.0000,4000.0),
      Row("Stock","Plus500",0.06,"Facebook","01/11/2021",160.0000,200.0),
      Row("Stock","eToro",1.50,"Google","02/11/2021",180.00,230.0),
      Row("Crypto","Plus500",1.30,"Bitcoin","06/11/2021",45000.00,48000.0),
      Row("Crypto","Coinbase",0.01,"Ethereum","12/11/2021",3800.0000,4000.0),
      Row("Crypto","Plus500",0.06,"Chainlink","12/11/2021",20.00,50.0))

    print("Type of data:"+data.getClass)

    val schema=StructType(Array(StructField("Asset",StringType,true),
               StructField("Platform",StringType,true),
               StructField("Unit",DoubleType,true),
               StructField("TradeName",StringType,true),
               StructField("Buy Date",StringType,true),
               StructField("Buy Price",DoubleType,true),
               StructField("Sell Price",DoubleType,true)))
    val df=spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    df.printSchema()
    df.show(false)

    val StatsPerPlatform=df.groupBy("Platform")
                                    .pivot("Asset",List("Stock","Crypto"))
                                    .agg(sum("Unit").as("TotalUnit"),
                                         sum("Buy Price").as("TotalBuyPrice"),
                                         sum("Sell Price").as("TotalSellPrice"))
                                    .na.fill(0)
    StatsPerPlatform.show(false)

    //val perPlatformstats=StatsPerPlatform.groupBy("Platform").agg(
    //  sum("Stock_TotalUnit").as("Stock_TotalUnit_PerPlatform"),
    //  sum("Stock_TotalBuyPrice").as("Stock_TotalBuyPrice_PerPlatform"),
    //  sum("Stock_TotalSellPrice").as("Stock_TotalSellPrice_PerPlatform"),
    //  sum("Crypto_TotalUnit").as("Crypto_TotalUnit_PerPlatform"),
    //  sum("Crypto_TotalBuyPrice").as("Crypto_TotalBuyPrice_PerPlatform"),
    //  sum("Crypto_TotalSellPrice").as("Crypto_TotalSellPrice_PerPlatform"),

    //  max("Stock_TotalUnit").as("Stock_MaxUnit_PerPlatform"),
    //  max("Stock_TotalBuyPrice").as("Stock_MaxBuyPrice_PerPlatform"),
    //  max("Stock_TotalSellPrice").as("Stock_MaxSellPrice_PerPlatform"),
    //  max("Crypto_TotalUnit").as("Crypto_MaxUnit_PerPlatform"),
    //  max("Crypto_TotalBuyPrice").as("Crypto_MaxBuyPrice_PerPlatform"),
    //  max("Crypto_TotalSellPrice").as("Crypto_MaxSellPrice_PerPlatform"))

   // perPlatformstats.printSchema()
   // perPlatformstats.show(false)

    //Use map function by creating list of column names and mapping each column to aggregate functions instead of writing above code

    val columnnames=List("Stock_TotalUnit","Stock_TotalBuyPrice","Stock_TotalSellPrice","Crypto_TotalUnit","Crypto_TotalBuyPrice","Crypto_TotalSellPrice")

    val sumFunc=columnnames.map(colname=>sum(colname).as(colname+"_Sum_PerPlatform"))
    val maxFunc=columnnames.map(colname=>max(colname).as(colname+"_Max_PerPlatform"))
    val minFunc=columnnames.map(colname=>min(colname).as(colname+"_Min_PerPlatform"))
    val avgFunc=columnnames.map(colname=>avg(colname).as(colname+"_Avg_PerPlatform"))

    val aggExpression=sumFunc++maxFunc++minFunc++avgFunc
    aggExpression.foreach(println)


    val perPlatformStats=StatsPerPlatform.groupBy("Platform").agg(aggExpression.head,aggExpression.tail:_*)

    perPlatformStats.show(false)

  }

}
