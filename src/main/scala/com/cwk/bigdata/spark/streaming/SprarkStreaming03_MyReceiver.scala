package com.cwk.bigdata.spark.streaming

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SprarkStreaming03_MyReceiver {


  def main(args: Array[String]): Unit = {
    //使用sparkstreaming完成WordCount


    //spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SprarkStreaming01_WordCount")


    //实时数据分析环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val stramingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))



    //从制定的端口中采集数据


//    val value1: ReceiverInputDStream[String] = stramingContext.receiverStream(new MyReceiver("hadoop102", 9999))

    //将采集的数据扁平化
//    val wordStream: DStream[String] = value1.flatMap(line => line.split(" "))


//    //将数据进行结构统计
//    val mapDStream: DStream[(String, Int)] = wordStream.map((_, 1))
//
//    //将转换后的数据进行聚合处理
//    val value: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
//
//
//    value.print()


    //不能停止采集程序
//    stramingContext.stop()

    //启动采集器
    stramingContext.start()
    //Driver等待采集器执行

    stramingContext.awaitTermination()









  }
}
//声明采集器
class MyReceiver(host:String, post:Int) extends  Receiver(StorageLevel.MEMORY_ONLY){
  var socket: java.net.Socket = null


    def receive(): Unit = {
      socket = new java.net.Socket(host, post)


      val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

      var line = null


      while (false) {

        //将采集的数据存储到采集器的内部进行转换

        if ("END".equals(line)){

          return
        }else {
//          this.store(line)
        }

      }

    }
  override def onStart(): Unit = {
    new Thread(new Runnable {

      override def run(): Unit = {
        receive()
      }
    }).start()


  }


  override def onStop(): Unit = {

    if (socket != null) {
      socket.close()
      socket = null
    }


  }
}