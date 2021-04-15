package com.dahua.analyse

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TerritotyAna {

  def main(args: Array[String]): Unit = {
    // 判断参数。
    if (args.length != 1) {
      println(
        """
          |com.dahua.analyse.TerriotyAna
          |缺少参数
          |inputPath

        """.stripMargin)
      sys.exit()
    }

    // 接收参数
    val Array(inputPath) = args
    // 获取SparkSession
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val df: DataFrame = spark.read.parquet(inputPath)

    df.createTempView("log")

    var sql ="select provincename,cityname ," +
      "sum(case when requestmode =1 and processnode >=1 then 1 else 0 end)as ysqq ," +
      "sum(case when requestmode =1 and processnode >=2 then 1 else 0 end)as yxqq ," +
      "sum(case when requestmode =1 and processnode = 3 then 1 else 0 end )as ggqq," +
      "sum(case when iseffective =1 and isbilling = 1 and isbid =1 and adorderid != 0 then 1 else 0 end )as jjx," +
      "sum(case when iseffective =1 and isbilling = 1 and iswin =1  then 1 else 0 end )as jjcgs," +
      "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as zss," +
      "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as djs," +
      "sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss," +
      "sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs," +
      "sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei," +
      "sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben " +
      "from log " +
      "group by " +
      "provincename,cityname"
//    var sql1 ="select * from log"

    val df1: DataFrame = spark.sql(sql)
    val load :Config = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("driver",load.getString("jdbc.driver"))
    properties.setProperty("password",load.getString("jdbc.password"))
    df1.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName2"),properties)



    //展示
//    spark.sql(sql).show(50)

spark.stop()
  }
}
