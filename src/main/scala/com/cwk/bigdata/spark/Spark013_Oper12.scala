package com.cwk.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark013_Oper12 {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //map 算子
   // val ListRDD: RDD[Int] = sc.makeRDD(1 to 10)

    val ListRDD = sc.makeRDD(List(("a",1),("b",2),("c",3)))

    val partRDD: RDD[(String, Int)] = ListRDD.partitionBy(new Myparitioner(3))


    partRDD.saveAsTextFile("output")
  }



}

class Myparitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {


    1


  }
}
