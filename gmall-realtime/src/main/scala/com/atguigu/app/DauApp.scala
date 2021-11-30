package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.获取kafka的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //4.将JSON格式的数据转为样例类，并补全logDate&logHour两个时间字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将JSON数据转为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        val times: String = sdf.format(new Date(startUpLog.ts))
        //补全logDate
        startUpLog.logDate = times.split(" ")(0)
        //补全logHour
        startUpLog.logHour = times.split(" ")(1)
        startUpLog
      })
    })

    //优化：缓存多次使用的流
    startUpLogDStream.cache()

    //5.进行批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream)

    //优化：缓存多次使用的流
    filterByRedisDStream.cache()

    //打印原始数据的个数
    startUpLogDStream.count().print()

    //打印经过去批次间重后的数据个数
    filterByRedisDStream.count().print()

    //6.进行批次内去重

    //7.将去重后的（mid）保存到redis
   DauHandler.saveToRedis(filterByRedisDStream)


    //8.将去重后的明细数据保存到Hbase



//    //4.打印测试
//    val value: DStream[String] = kafkaDStream.mapPartitions(partition => {
//      partition.map(record => {
//        record.value()
//      })
//    })

//    value.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
