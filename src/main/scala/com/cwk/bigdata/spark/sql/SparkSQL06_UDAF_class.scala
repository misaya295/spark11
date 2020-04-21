package com.cwk.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._


object SparkSQL06_UDAF_class {


  def main(args: Array[String]): Unit = {


    //创建SparkConf()并设置App名称

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_01")

    //val spark: SparkSession = new SparkSession(config
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()


    //创建聚合函数对象
    val udaf = new MyAgeAvgFunciton1

    //将聚合函数转换为查询的咧
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")

    val frame: DataFrame =  spark.read.json("in/user.json")


    import spark.implicits._
    val userDs: Dataset[UserBean] = frame.as[UserBean]


    userDs.select(avgCol).show()

    spark.stop()


  }

}


case class UserBean(name: String, age:BigInt)
case class AvgBuffer(var sum : BigInt, var count :Int)
//声明用户的自定义函数(强类型)
//继承Aggregator，设定泛型
//继承
//实现方法


class MyAgeAvgFunciton1 extends Aggregator [ UserBean, AvgBuffer, Double]{
  override def zero: AvgBuffer = {

    AvgBuffer(0,0)

  }
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
      b.sum = b.sum + a.age
      b.count = b.count + 1
      b
  }


  //缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count

    b1
  }


  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count





  }


  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

































