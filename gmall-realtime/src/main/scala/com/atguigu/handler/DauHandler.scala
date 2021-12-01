package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 批次内去重
    * @param filterByRedisDStream
    */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    //1.先将数据转为K，v格式的数据
    val midWithLogDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(startUplog => {
      ((startUplog.mid, startUplog.logDate), startUplog)
    })

    //2.按照Key进行聚合
    val midWithLogDateIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midWithLogDateToLogDStream.groupByKey()

    //3.对相同key的数据进行排序
    val midWithLogDateToListLogDStream: DStream[((String, String), List[StartUpLog])] = midWithLogDateIterLogDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //4.获取list集合中每一个元素
    val value: DStream[StartUpLog] = midWithLogDateToListLogDStream.flatMap(_._2)
    value
  }

  /**
    * 进行批次间去重
    *
    * @param startUpLogDStream
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
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
    /* val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
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
     value*/
    //方案三：在每个批次内获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1.一个批次执行一次  在Driver端执行
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //2.获取redis中的数据
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //3.将Driver端查出来的mid广播到executor端去重
      val midsBc: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //4.去重
      val midRDD: RDD[StartUpLog] = rdd.filter(startUplog => {
        //获取到广播的数据
        !midsBc.value.contains(startUplog.mid)
      })
      midRDD
    })
    value
  }

  /**
    * 将去重后的（mid）保存到redis
    *
    * @param startUpLogDStream
    */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {

    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //在每个分区下创建redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        partition.foreach(log => {
          val redisKey: String = "DAU:" + log.logDate
          jedis.sadd(redisKey, log.mid)
        })
        jedis.close()
      })
    })

  }

}
