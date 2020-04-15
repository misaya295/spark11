package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Oper7 {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //生成数据，按照制定的规则进行分组
    val ListRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


    val FilterRDD: RDD[Int] = ListRDD.filter(x => x % 2 == 0)


    FilterRDD.collect().foreach(println)



//    tupleRDD.collect().foreach(println)



  }
}
