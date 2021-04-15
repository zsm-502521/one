package com.dahua.analyse

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityCount {
  def main(args: Array[String]): Unit = {
    //判断参数
    if(args.length!=2){
      println(
      """
        |com.dahua.analyse.ProCityCount
        |缺少参数
        |inputPath
        |outputPath
      """.stripMargin)
      sys.exit()
    }
    var  Array(intputPath,outputPath) =args
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()
    val sc: SparkContext = spark.sparkContext

//   读取数据源
    val df:DataFrame = spark.read.parquet(intputPath)

    //创建 临时视图
    df.createTempView("log")

    val sql = " select provincename,cityname,count(*) from log group by provincename,cityname"
    val procityCount:DataFrame = spark.sql(sql)

//打印输出json数据
    procityCount.write.partitionBy("provincename","cityname").json(outputPath)
    spark.stop()
  }
}
