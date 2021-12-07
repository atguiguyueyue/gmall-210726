package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建StreamIngContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.消费kafka数据
    //获取订单表的数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

    //获取订单明细表的数据
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL,ssc)


    //4.分别将两张表的数据转为样例类
    val orderInfoDStream = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补全字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

        (orderInfo.id,orderInfo)
      })
    })

    //订单明细表数据
    val orderDetailDStream = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id,orderDetail)
      })
    })

//    orderInfoDStream.print()

//    orderDetailDStream.print()

    //5.对两条流进行join
//    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)
//    value.print()

    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.利用缓存的方式处理因网络延迟所带来数据丢失的问题
    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(partition => {

      implicit val formats = org.json4s.DefaultFormats

      //创建结果集合
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      partition.foreach { case (orderId, (infoOpt, detailOpt)) =>

        //orderInfoRedisKey
        val orderInfoRedisKey: String = "orderInfo:" + orderId

        //orderDetailRedisKey
        val orderDetailRediskey: String = "orderDetail:" + orderId

        //TODO a.判断orderInfo数据是否存在
        if (infoOpt.isDefined) {
          //orderInfo存在
          val orderInfo: OrderInfo = infoOpt.get
          //TODO a.2判断orderDetail是否存在
          if (detailOpt.isDefined) {
            //orderDetail存在
            val orderDetail: OrderDetail = detailOpt.get

            val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            //将关联后的样例类存入结果集合
            details.add(detail)
          }

          //TODO b.将orderInfo数据写入缓存（redis）
          //b.2 将orderInfo数据转为JSON串
          //样例类不能用方式直接转为JSON串
          //          val str: String = JSON.toJSONString(orderInfo)
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(orderInfoRedisKey, orderInfoJson)
          //对数据设置一个过期时间，防止内存不够
          jedis.expire(orderInfoRedisKey, 20)

          //TODO c.查询对方缓存中是否有对应的orderDetail数据
          //c.2判断rediskey是否存在
          if (jedis.exists(orderDetailRediskey)) {
            //取出orderDetail数据
            val detailsJsonStr: util.Set[String] = jedis.smembers(orderDetailRediskey)
            //将java集合转为Scala集合
            for (elem <- detailsJsonStr.asScala) {
              //将查询出来的json字符串的orderDetail数据转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            }
          }
        } else {
          //orderInfo不在
          //TODO d.判断OrderDetail是否存在
          if (detailOpt.isDefined) {
            val orderDetail: OrderDetail = detailOpt.get
            //d.2 去对方缓存中查询是否有对应的orderInfo数据
            //判断orderInfo的rediskey是否存在
            if (jedis.exists(orderInfoRedisKey)) {
              //有orderInfo数据，并取出
              val infoJsonStr: String = jedis.get(orderInfoRedisKey)
              //将查询出的字符串转为样例类
              val orderInfo: OrderInfo = JSON.parseObject(infoJsonStr, classOf[OrderInfo])
              //组和成为SaleDetail样例类
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              //将SaleDetail写入结果集合
              details.add(detail)
            } else {
              //TODO e.对方缓存中没有能关联上的数据，则把自己(orderDetail)写入Redis缓存\
              //将样例类转为JSON字符串
              val detailJsonStr: String = Serialization.write(orderDetail)
              jedis.sadd(orderDetailRediskey, detailJsonStr)
              //对其设置过期时间
              jedis.expire(orderDetailRediskey, 20)
            }
          }
        }
      }
      //在每个分区下关闭redis连接
      jedis.close()
      //将结果集合转为Scala集合并转为迭代器返回
      details.asScala.toIterator
    })

    //打印测试，双流join+缓存的方式是否解决了因网络延迟所带来的数据丢失问题
//    noUserSaleDetailDStream.print()

    //7.反查缓存，关联userInfo数据
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(partition => {
      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //1.取用户表的数据
        val userInfoRedisKey: String = "userInfo:" + saleDetail.user_id

        val userInfoJsonStr: String = jedis.get(userInfoRedisKey)

        //2.将数据转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])

        //3.关联userInfo数据
        saleDetail.mergeUserInfo(userInfo)

        saleDetail
      })
      jedis.close()
      details
    })
    saleDetailDStream.print()

    //8.将关联后的明细数据写入ES
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          //将订单明细表的id作为es的docid，因为它不会重复，粒度更小
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_SALEDETAIL_PREFIX+"0726",list)
      })
    })



    ssc.start()
    ssc.awaitTermination()

  }

}
