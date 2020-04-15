package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper5 {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //map 算子
    val ListRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2), List(3, 4)))

    val faltMapRDD: RDD[Int] = ListRDD.flatMap(datas => datas)


    faltMapRDD.collect().foreach(println)




//    tupleRDD.collect().foreach(println)



  }
}
