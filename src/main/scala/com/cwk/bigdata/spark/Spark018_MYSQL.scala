package com.cwk.bigdata.spark

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON


object Spark018_MYSQL {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)



    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://linux01:3306/rdd"
    val userName = "root"
    val passWd = "000000"



    //创建jdbcRDD
    //查询数据
//    val sql= "select name, age,from user where id >= ? and id <= ?"
//    val jdbcRDD = new JdbcRDD(
//
//      sc,
//      () => {
//
//        //获取数据库连接对象
//        Class.forName(driver)
//        java.sql.DriverManager.getConnection(url, userName, passWd)
//      },
//      sql,
//      1,
//      3,
//      2,
//      (rs) => {
//
//        println(rs.getString(1) + "," + rs.getInt(2))
//
//      }
//
//    )
//      jdbcRDD.collect()

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zhan", 20), ("lisi", 30),("dadasd",2)),2)

    /*
    dataRDD.foreach{
      case (username, age) => {
        Class.forName(driver)
        val connection: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)
        val sql = "insert into user (name,age) values (?,?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setString(1,username)
        statement.setInt(2,age)
        statement.executeUpdate()
        statement.close()
        connection.close()
      }
    }
*/
    dataRDD.foreachPartition(datas => {

      Class.forName(driver)
      val connection: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)
        datas.foreach{

          case (username ,age ) => {
            val sql = "insert into user (name,age) values (?,?)"
            val statement: PreparedStatement = connection.prepareStatement(sql)
            statement.setString(1,username)
            statement.setInt(2,age)
            statement.executeUpdate()
            statement.close()
            }
          }
          connection.close()
      })
        sc.stop()
  }
}
