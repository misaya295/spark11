package com.cwk.bigdata.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}


object Spark021_Accumulator {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 34, 4), 2)

    //    val i: Int = dataRDD.reduce(_+_)


    //使用累加器来共享变量


    // TODO 创建累加器


    val wordAccumulator = new WordAccumulator
    // TODO 注册累加器
    sc.register(wordAccumulator)


    val value: RDD[String] = sc.makeRDD(List("hadoop", "hive", "hbase"), 2)


    val accumulator: LongAccumulator = sc.longAccumulator

    value.foreach {

      case word => {

        wordAccumulator.add(word)
      }
    }



    //TODO 获取累加器的值
    println(wordAccumulator.value)


    sc.stop()
  }
}


//声明累加器
class WordAccumulator extends AccumulatorV2[String , util.ArrayList[String]] {

  val list = new util.ArrayList[String]()


  //当前累加器是否为初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {

    new WordAccumulator()

  }


  //重置
  override def reset(): Unit = {
    list.clear()
  }

  //向累加器中增加数据
  override def add(v: String): Unit = {

    if (v.contains("h")){
      list.add(v)
    }


  }

  //合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  override def value: util.ArrayList[String] = {


    list


  }
}
