package com.cwk.bigdata.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark020_shareData {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 34, 4), 2)

//    val i: Int = dataRDD.reduce(_+_)


    //使用累加器来共享变量

    var sum = 0
    dataRDD.foreach(i => sum = sum + i)

    println(sum)


    sc.stop()
  }
}
