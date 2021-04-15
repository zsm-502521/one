package com.dahua.analyse

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TerritotyAngV2{

  def ysqq(reqMode:Int,proNode:Int):List[Double]={
    if(reqMode ==1 && proNode ==1){
      List[Double](1,0)
    }else if(reqMode ==1 && proNode ==2){
      List[Double](1,1)
    }else{
      List[Double](0,0)
    }
  }

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
    import spark.implicits._

    // 关闭对象。
    val sc: SparkContext = spark.sparkContext
    val df: DataFrame = spark.read.parquet(inputPath)
    // 获取每个列。
    val res: RDD[((String, String), List[Double])] = df.map(row => {
      // 获取列。
      val requestMode: Int = row.getAs[Int]("requestmode")
      val processNode: Int = row.getAs[Int]("processnode")
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      var num = List[Double]()
      num = ysqq(requestMode,processNode)
      ((province, cityname), num ) //  (1,0)  (1,1)

      //(1,1),(0,1)
    }).rdd.reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
//    res.foreach(println(_))
    res.saveAsTextFile(outputPath)

  }

}
