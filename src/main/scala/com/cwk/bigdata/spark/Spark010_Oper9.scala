package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark010_Oper9 {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //生成数据，按照制定的规则进行分组
    val ListRDD: RDD[Int] = sc.makeRDD(List(1,1,1,1,1,1,1,4,3,4,4,1234,4,123,124,124,54,3,1,7))


    val DRDD: RDD[Int] = ListRDD.distinct()


//    DRDD.collect().foreach(println)
    DRDD.saveAsTextFile("output")

  }
}
