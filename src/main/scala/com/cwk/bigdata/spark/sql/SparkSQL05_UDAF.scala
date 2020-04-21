package com.cwk.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}


object SparkSQL05_UDAF {


  def main(args: Array[String]): Unit = {


    //创建SparkConf()并设置App名称

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_01")

    //val spark: SparkSession = new SparkSession(config
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()

    import spark.implicits._


    //自定义聚合函数

    //创建聚合函数对象
    val uadf = new MyAgeAvgFunciton
    spark.udf.register("AvgAge", uadf)

    //使用
    val frame: DataFrame =  spark.read.json("in/user.json")

    frame.createOrReplaceTempView("user")

    spark.sql("select AvgAge(age) from user").show()

    spark.stop()


  }

}

//声明用户的自定义函数

//继承
//实现方法
class MyAgeAvgFunciton extends UserDefinedAggregateFunction{

  //函数输入的数据结构
  override def inputSchema: StructType = {


    new StructType().add("age",LongType)

  }

  // 计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)

  }
  //函数返回的数据类型
  override def dataType: DataType = DoubleType


  //函数是否稳定
  override def deterministic: Boolean = true


  //计算钱缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit ={

    buffer(0) = 0L
    buffer(1) = 0L


  }


  //更新数据 ，根据查询结果更新缓冲区的数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1



  }


  //将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)


  }


  //计算
  override def evaluate(buffer: Row): Any = {

    buffer.getLong(0).toDouble / buffer.getLong(1)



  }
}

































