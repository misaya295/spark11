package com.cwk.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL03_SQL {


  def main(args: Array[String]): Unit = {




    //创建SparkConf()并设置App名称

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_01")
      
    //val spark: SparkSession = new SparkSession(config
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()

    import spark.implicits._


    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 3), (1, "lisi", 33), (3, "das", 33)))

    //转换为DF

    val df: DataFrame = rdd.toDF("id", "name", "age")


    //转换为DS


    val ds: Dataset[User] = df.as[User]



    //转换为DF

    val df1: DataFrame = ds.toDF()


    //转换为RDD


    val rdd1: RDD[Row] = df1.rdd



    rdd1.foreach(
      row => {


        //获取数据时通过索引访问数据
        println(row.getString(1))

      }


    )



    spark.stop()
    

 }




}

case class User(id:Int,name: String,age:Int)