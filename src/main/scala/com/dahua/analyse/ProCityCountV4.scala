package com.dahua.analyse

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityCountV4 {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println(
        """
          |com.dahua.analyse.ProCityCount
          |缺少参数
          |inputPath
        """.stripMargin
      )
      sys.exit()
    }
    // 接收参数
    val Array(inputPath) = args
    // 获取SparkSession
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val sc: SparkContext = spark.sparkContext
    // 需求1： 统计各个省份分布情况，并排序。

    // 需求2： 统计各个省市分布情况，并排序。

    // 需求3： 使用RDD方式，完成按照省分区，省内有序。

    // 需求4： 将项目打包，上传到linux。使用yarn_cluster 模式进行提交，并查看UI。

    // 需求5： 使用azkaban ，对两个脚本进行调度。

    //读取数据源
    val df: DataFrame = spark.read.parquet(inputPath)
    //创建临时试图
    df.createTempView("log")
    //sql  ouput02
//    var sql1 = "select " +
//      "provincename,count(*) as prosum " +
//      "from log " +
//      "group by provincename " +
//      "order by prosum"

    var sql2="select " +
      "provincename ,cityname, " +
      "row_number() over(partition by provincename order by cityname) as pcount " +
      "from log " +
      "group by provincename,cityname"
    spark.sql(sql2).show(50)


    spark.stop()

  }

}
