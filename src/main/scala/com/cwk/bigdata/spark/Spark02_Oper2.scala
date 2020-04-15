package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper2 {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //map 算子
    val ListRDD: RDD[Int] = sc.makeRDD(1 to 10)


    //mapPartitions可以对一个RDD中所有的分区进行遍历
    //mapPartitions效率优于map算子，减少了发送到执行器执行交互次数
    //mapPartitions可能会出现内存溢出（OOM）
    val mapPartitionsRDD: RDD[Int] = ListRDD.mapPartitions(datas => {


      datas.map(_ * 2)


    })



    mapPartitionsRDD.collect().foreach(println)




  }
}
