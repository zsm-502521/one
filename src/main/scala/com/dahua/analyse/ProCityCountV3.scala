package com.dahua.analyse

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ProCityCountV3 {

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
    val mf: RDD[Array[String]] = rdd.map(line=> line.split(",",-1)).filter(arr=>arr.length>=85)
    val proCity1: RDD[((String, String), Int)] = mf.map(field => {
      val pro = field(24)
      val city = field(25)
      ((pro, city), 1)
    })
    val reduceByKey: RDD[((String, String), Int)] = proCity1.reduceByKey(_+_)
    val res: RDD[String] = reduceByKey.map(res => {
      res._1._1+"\t"+ res._1._2+"\t"+res._2
    })

    res.foreach(println)
//    res.saveAsTextFile(outputPath)
    // 关闭对象。
    spark.stop()


  }

}
