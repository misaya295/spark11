package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark016_checkpoint {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    sc.setCheckpointDir("op")

    //生成数据，按照制定的规则进行分组
    val ListRDD: RDD[Int] = sc.makeRDD(1 to 16)


    val mapRDD: RDD[(Int, Int)] = ListRDD.map((_, 1))

    mapRDD.checkpoint()

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

    println(reduceRDD.toDebugString)





  }
}
