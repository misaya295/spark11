package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //创建RDD
    // 1）从内存中创建 makeRDD 底层实现parallelize
    val ListRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)


    //2）从内存中创建 parallelize
//    val ArrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3))


    //3)从外部存储中创建
    //默认情况下可以读取项目路径，也可以读取其他路径 HDFS hdfs://*******
    //默认从文件中读取都是字符串类型
    val fileRDD: RDD[String] = sc.textFile("in",3)
//
//    ListRDD.collect().foreach(println)



    //将RDD的数据保存到文件中
    fileRDD.saveAsTextFile("output")



  }

}
