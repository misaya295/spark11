package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {


    //使用开发工具完成spark的wordcount开发

    //lcoal模式
    //设定spark计算框架的运行环境
    //app id
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")


    //创建spark上下文对象
    val sc = new SparkContext(config)


    //读取文件



    //本地写法：file:///opt/module/spark/in
    val lines: RDD[String] = sc.textFile("file:///opt/module/spark/in")

    //将一行行数据扁平化
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val word2one: RDD[(String, Int)] = words.map((_, 1))


    //分组聚合
    val wordToSum: RDD[(String, Int)] = word2one.reduceByKey(_ + _)


    val result: Array[(String, Int)] = wordToSum.collect()

//    println(result)
    result.foreach(println)



    sc.stop()






  }



}
