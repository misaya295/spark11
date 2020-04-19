package com.cwk.bigdata.spark

import java.sql.{Connection, PreparedStatement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark019_Hbase {


  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val conf: Configuration = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    conf.set(TableInputFormat.INPUT_TABLE, "student")

    println("连接成功？")
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hbaseRDD.foreach{

      case (rowkey, result) => {
        val cells: Array[Cell] = result.rawCells()
        for (cell <- cells) {
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }




    sc.stop()
  }
}
