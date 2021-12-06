package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

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
    fullJoinDStream.mapPartitions(partition=>{

      implicit val formats=org.json4s.DefaultFormats

      //创建结果集合
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102",6379)

      partition.foreach{case (orderId,(infoOpt,detailOpt))=>

        //orderInfoRedisKey
        val orderInfoRedisKey: String = "orderInfo:"+orderId

          //TODO a.判断orderInfo数据是否存在
        if (infoOpt.isDefined){
          //orderInfo存在
          val orderInfo: OrderInfo = infoOpt.get
          //TODO a.2判断orderDetail是否存在
          if (detailOpt.isDefined){
            //orderDetail存在
            val orderDetail: OrderDetail = detailOpt.get

            val detail: SaleDetail = new SaleDetail(orderInfo,orderDetail)
            //将关联后的样例类存入结果集合
            details.add(detail)
          }

          //TODO b.将orderInfo数据写入缓存（redis）
          //b.2 将orderInfo数据转为JSON串
          //样例类不能用方式直接转为JSON串
//          val str: String = JSON.toJSONString(orderInfo)
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(orderInfoRedisKey,orderInfoJson)
          //对数据设置一个过期时间，防止内存不够
          jedis.expire(orderInfoRedisKey,20)

        }

      }
      jedis.close()
      partition
    })



    ssc.start()
    ssc.awaitTermination()

  }

}
