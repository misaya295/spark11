package com.cwk.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL04_Transform1 {


  def main(args: Array[String]): Unit = {




    //创建SparkConf()并设置App名称

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_01")
      
    //val spark: SparkSession = new SparkSession(config
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()

    import spark.implicits._


    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 3), (1, "lisi", 33), (3, "das", 33)))


    //RDD->DataSet
    val UserRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)

      }


    }


    val usDS: Dataset[User] = UserRDD.toDS()


    val userrdd: RDD[User] = usDS.rdd

    userrdd.foreach(println)


    spark.stop()
    

 }




}

