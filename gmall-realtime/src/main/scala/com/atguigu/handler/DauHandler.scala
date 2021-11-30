package com.atguigu.handler

import java.lang

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 进行批次间去重
    * @param startUpLogDStream
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    /*val value: DStream[StartUpLog] = startUpLogDStream.filter(startUplog => {
      //1.获取redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      val redisKey: String = "DAU:" + startUplog.logDate

      //2.利用redis中的方法判断当前的mid在redis中是否存在
      val boolean: lang.Boolean = jedis.sismember(redisKey, startUplog.mid)

      jedis.close()

      !boolean
    })
    value*/
    //方案二：在每个分区下获取连接，以减少连接个数
    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      //在分区下创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = partition.filter(startUplog => {
        val redisKey: String = "DAU:" + startUplog.logDate

        //2.利用redis中的方法判断当前的mid在redis中是否存在
        val boolean: lang.Boolean = jedis.sismember(redisKey, startUplog.mid)

        !boolean
      })
      jedis.close()
      logs
    })
    value
  }

  /**
    * 将去重后的（mid）保存到redis
    *
    * @param startUpLogDStream
    */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {

    startUpLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //在每个分区下创建redis连接
        val jedis: Jedis = new Jedis("hadoop102",6379)
        partition.foreach(log=>{
          val redisKey: String = "DAU:"+log.logDate
          jedis.sadd(redisKey,log.mid)
        })
        jedis.close()
      })
    })

  }

}
