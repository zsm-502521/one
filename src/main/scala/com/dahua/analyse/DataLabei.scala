package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.{JedisUtil, TerritoryTool}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object DataLabei {
  def getkey (imei:String,mac:String,idfa:String,openudid:String,androidid:String)={
    if (!imei.isEmpty){
      imei
    }else if(!mac.isEmpty){
      mac
    }else if (!idfa.isEmpty){
      idfa
    }else if (!openudid.isEmpty){
      idfa
  }
}
  def main(args: Array[String]): Unit = {
    // 判断参数。
    if (args.length != 1) {
      println(
        """
          |com.dahua.analyse.ProCityCount
          |缺少参数
          |inputPath
        """.stripMargin)
      sys.exit()
    }

    // 接收参数
    val Array(inputPath) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile(inputPath)

    val rdd1: RDD[Array[String]] = rdd.map(line => {
      line.split(",", -1)
    }).filter(_.length >= 85)
    val rdd2: RDD[Unit] = rdd1.map(Log(_)).map(line => {
      val key = getkey(line.imei, line.mac, line.idfa, line.openudid,line.androidid)
//      (key,Log.apply())
    })
rdd2.foreach(println)

  }
}
