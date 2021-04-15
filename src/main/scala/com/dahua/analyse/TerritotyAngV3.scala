package com.dahua.analyse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.dahua.util.TerritoryTool

object TerritotyAngV3{

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
    val df: DataFrame = spark.read.parquet(inputPath)

    //获取每个列
    val rdd: Dataset[((String, String), List[Double])] = df.map(row => {
      // 获取列。
      val requestMode: Int = row.getAs[Int]("requestmode")
      val processNode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      val appname: String = row.getAs[String]("appname")

      // 获取各个指标。
      val qqs: List[Double] = TerritoryTool.qqsRtp(requestMode, processNode)
      val jingjia: List[Double] = TerritoryTool.jingjiaRtp(iseffective, isbilling, isbid, iswin, adorderid)
      val ggz: List[Double] = TerritoryTool.ggzjRtp(requestMode, iseffective)
      val mj: List[Double] = TerritoryTool.mjjRtp(requestMode, iseffective, isbilling)
      val ggc: List[Double] = TerritoryTool.ggcbRtp(iseffective, isbilling, iswin, winprice, adpayment)
      // 从广播变量中获取值。如果为空。替换成新的appname
      ((province, cityname), qqs ++ jingjia ++ ggz ++ mj ++ ggc)

    })
    rdd.rdd.reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).foreach(println(_))


//    res.foreach(println(_))
//    res.saveAsTextFile(outputPath)

  }

}
