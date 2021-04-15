package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.TerritoryTool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MeitiAna {
  def main(args: Array[String]): Unit = {
    if(args.length!=3) {
      println(
        """
          |com.dahua.analyes.MeitiAna
          |缺少参数
          |inputPath
          |appmapping
          |outputPath
        """.stripMargin)
      sys.exit()
  }
      //      接受参数
      val Array(inputPath, appmapping, outputPath) = args
      //      获取SparkSession
      val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val spark: SparkSession = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
      val sc: SparkContext = spark.sparkContext
      val mapping: RDD[String] = sc.textFile(appmapping)
      val map: Map[String, String] = mapping.map(line => {
        val arr: Array[String] = line.split("[:]", -1)
        (arr(0), arr(1))
      }).collect().toMap
      val broadcase: Broadcast[Map[String, String]] = sc.broadcast(map)

      val rdd: RDD[String] = sc.textFile(inputPath)
      rdd
      //分析业务
      val log: RDD[Log] = rdd.map(_.split(",", -1)).filter(_.length >= 85).map(Log(_)).filter(t => !t.appid.isEmpty || !t.appname.isEmpty)
      val res: RDD[(String, List[Double])] = log.map(log => {
        val qqs: List[Double] = TerritoryTool.qqsRtp(log.requestmode, log.processnode)
        //媒体名称
        var appname: String = log.appname
        if (appname == "" || appname.isEmpty) {
          //从广播变量中获得
          appname = broadcase.value.getOrElse(log.appid, "不明确")
        }
        (appname, qqs)
      }).reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)
      })
//    res.foreach(println)
      res.saveAsTextFile(outputPath)

    sc.stop()
  }

}
