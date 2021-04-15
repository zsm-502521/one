package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.TerritoryTool
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TerritotyAngV4{

  def main(args: Array[String]): Unit = {
    // 判断参数。
    if (args.length != 1) {
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
    val Array(inputPath) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._

    // 关闭对象。
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile(inputPath)
    val log: RDD[Log] = rdd.map(_.split(",",-1)).filter(_.length>=85).map(Log(_))
    log.map(log=>{
      val qqs: List[Double] = TerritoryTool.qqsRtp(log.requestmode,log.processnode)

    })
    log.foreach(println(_))

    


//    res.foreach(println(_))
//    res.saveAsTextFile(outputPath)
sc.stop()
  }

}
