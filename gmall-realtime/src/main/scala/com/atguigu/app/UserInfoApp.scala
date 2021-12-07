package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")

    //2.创建StreamIngContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.从kafka获取数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER,ssc)

    //4.将数据写入Redis中
    kafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //获取redis连接
        val jedis: Jedis = new Jedis("hadoop102",6379)
        partition.foreach(record=>{
          //将UserInfo的json字符串数据写入Redis
          val userInfo: UserInfo = JSON.parseObject(record.value(),classOf[UserInfo])
//          println(userInfo)
          val userInfoRedisKey: String = "userInfo:"+userInfo.id
          jedis.set(userInfoRedisKey,record.value())

        })
        jedis.close()
      })
    })

    //将数据转为样例类
    val userInfoDStream: DStream[UserInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })
    userInfoDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
