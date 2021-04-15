package com.dahua.analyse

import com.dahua.bean.Log
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProCityCountv5 {

  def main(args: Array[String]): Unit = {
    // 判断参数。
    if (args.length != 2) {
      println(
        """
          |com.dahua.analyse.ProCityCount
          |缺少参数
          |inputPath
          |outputPath
        """.stripMargin)
      sys.exit()
    }

    // 接收参数
    val Array(inputPath,outputPath) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile(inputPath)
    val rdd1: RDD[(String, List[(String, Int)])] = rdd.map(_.split(",", -1)).filter(x => {
      x.length >= 85
    }).map(Log(_)).map(line => {
      ((line.provincename, line.cityname), 1)
    }).reduceByKey(_ + _).groupBy(x => x._1._1).map(line => {
      (line._1, line._2.toMap.map(x => {
        (x._1._2, x._2)
      }).toList.sortBy(_._2).reverse)
    })
    rdd1.foreach(println)
    // 关闭对象。
    spark.stop()
  }
}
