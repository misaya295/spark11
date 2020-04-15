package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Oper8 {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //生成数据，按照制定的规则进行分组
    val ListRDD: RDD[Int] = sc.makeRDD(1 to 10)


    val sampleRDD: RDD[Int] = ListRDD.sample(false,0.4, 1)



    sampleRDD.collect().foreach(println)


  }
}
