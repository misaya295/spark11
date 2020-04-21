package com.cwk.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper1 {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //map 算子
    val ListRDD: RDD[Int] = sc.makeRDD(1 to 10)

    val mapRDD: RDD[Int] = ListRDD.map( _ * 2)

    mapRDD.collect().foreach(println)






  }
}
