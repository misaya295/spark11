package com.cwk.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkSQL02_Transform {


  def main(args: Array[String]): Unit = {




      //创建SparkConf()并设置App名称

      val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_01")
      
    //val spark: SparkSession = new SparkSession(config
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()


      val frame: DataFrame =  spark.read.json("in/user.json")

    frame.createOrReplaceTempView("user")

    //采用sql语法
//      frame.show()

    spark.sql("select * from user").show()

      spark.stop()
    

 }




}

