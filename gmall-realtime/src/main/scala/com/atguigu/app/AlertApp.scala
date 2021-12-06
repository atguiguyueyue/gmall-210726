package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.从kafka获取事件日志数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

    //4.将kafka中消费到的数据转为样例类，并补全字段，并将数据转为k（mid），v（数据本身）类型的数据
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

        val times: String = sdf.format(new Date(eventLog.ts))

        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)

        (eventLog.mid, eventLog)
      })
    })

    //5.开启一个5min的滑动窗口  如果没有设置滑动步长根据源码中注释可以分析得到，默认滑动步长就是批次时间 SparkStreaming默认就是滚动窗口，窗口大小为批次时间
    val midToWindowLogDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //6.按照相同的mid进行聚合
    val midToIterLogDStream: DStream[(String, Iterable[EventLog])] = midToWindowLogDStream.groupByKey()

    //7.根据条件筛选数据
    val boolToCouponAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterLogDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>

        //用来保存领优惠券的用户
        val uids: util.HashSet[String] = new util.HashSet[String]()

        //用来保存领优惠券所涉及的商品
        val itemIds: util.HashSet[String] = new util.HashSet[String]()

        //来用保存用户所涉及的事件
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //创建一个标志位，用来判断是否有浏览商品行为
        var bool: Boolean = true

        breakable {
          iter.foreach(log => {
            events.add(log.evid)
            //判断用户是否有浏览商品行为
            if ("clickItem".equals(log.evid)) {
              //有浏览商品
              bool = false
              //跳出循环不做处理
              break()
            } else if ("coupon".equals(log.evid)) {
              //对用户（uid）做去重
              uids.add(log.uid)
              itemIds.add(log.itemid)
            }

          })
        }
        //生成疑似预警日志
        (bool && uids.size() >= 3, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })

    //8.生成预警日志
    val counponAlertInfo: DStream[CouponAlertInfo] = boolToCouponAlertInfoDStream.filter(_._1).map(_._2)
    counponAlertInfo.print()

    //9.将预警日志写入ES
    counponAlertInfo.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, CouponAlertInfo)] = partition.toList.map(log => {
          (log.mid + log.ts / 1000 / 60, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_ALERT_PREFIX+"210726",list)
      })
    })

    //10.开启任务并阻塞
    ssc.start()
    ssc.awaitTermination()
  }

}
