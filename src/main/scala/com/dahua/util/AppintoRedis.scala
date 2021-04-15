package com.dahua.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object AppintoRedis {

  def main(args: Array[String]): Unit = {
    // 判断参数。
    if (args.length !=1) {
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
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile(inputPath)
    rdd.map(line => {
      val word: Array[String] = line.split("[:]")
      (word(0), word(1))
    }).foreachPartition(ite => {
      // 建立连接。
      var jedis = JedisUtil.resource
      ite.foreach(mapping => {
        jedis.set(mapping._1, mapping._2)
      })
      jedis.close()
    })


  }

}
