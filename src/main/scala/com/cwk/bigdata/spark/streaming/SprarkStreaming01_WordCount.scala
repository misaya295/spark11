package com.cwk.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SprarkStreaming01_WordCount {


  def main(args: Array[String]): Unit = {
    //使用sparkstreaming完成WordCount


    //spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SprarkStreaming01_WordCount")


    //实时数据分析环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val stramingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))



    //从制定的端口中采集数据

    val socketLineStream: ReceiverInputDStream[String] = stramingContext.socketTextStream("hadoop102", 9999)


    //将采集的数据扁平化
    val wordStream: DStream[String] = socketLineStream.flatMap(line => line.split(" "))


    //将数据进行结构统计
    val mapDStream: DStream[(String, Int)] = wordStream.map((_, 1))

    //将转换后的数据进行聚合处理
    val value: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)


    value.print()


    //不能停止采集程序
//    stramingContext.stop()

    //启动采集器
    stramingContext.start()
    //Driver等待采集器执行

    stramingContext.awaitTermination()









  }
}
