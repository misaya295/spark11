package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Oper6 {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //生成数据，按照制定的规则进行分组
    val ListRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


    val groupByRDD: RDD[(Int, Iterable[Int])] = ListRDD.groupBy(i => i % 2)


    groupByRDD.collect().foreach(println)





    //将一个分区的数据放到一个数组中
    val glomRDD :RDD[Array[Int]] = ListRDD.glom()

    glomRDD.collect().foreach(array => {


      println(array.mkString(","))



    })


//    tupleRDD.collect().foreach(println)



  }
}
