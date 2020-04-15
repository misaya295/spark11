package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Oper5 {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //map 算子
    val ListRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)


    //将一个分区的数据放到一个数组中
    val glomRDD :RDD[Array[Int]] = ListRDD.glom()

    glomRDD.collect().foreach(array => {


      println(array.mkString(","))



    })


//    tupleRDD.collect().foreach(println)



  }
}
