package app

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.wang930126.cat.common.constants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{RealtimeKafkaUtil, RedisUtil}

import scala.collection.mutable.ListBuffer

import util.RealtimeESUtil

import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import java.util

/**
  * 订单信息表 订单详情表 用户信息表 join 写入 es
  */
object RealTime_OrderAnalysis {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("order_analysis").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

        // TODO 1.从Kafka中获取订单信息 订单详情 的 两条实时流 以备join
        val infoInputDStream = RealtimeKafkaUtil.getKafkaStream(ssc, constants.KAFKA_TOPIC_ORDER)
        val detailInputDStream = RealtimeKafkaUtil.getKafkaStream(ssc,constants.KAFKA_TOPIC_ORDER_DETAIL)

        // TODO 2.把两条流分别整成样例类
        val infoDStream: DStream[OrderInfo] = infoInputDStream.map {
            record => {
                val jsonString: String = record.value()
                val info: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
                info.consignee_tel = info.consignee_tel.substring(0,4).concat("*******")
                info.create_date = info.create_time.split(" ")(0)
                info.create_hour = info.create_time.split(" ")(1).split(":")(0)
                info
            }
        }

        val detailDStream: DStream[OrderDetail] = detailInputDStream.map {
            record => {
                val jsonString: String = record.value()
                val detail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
                detail
            }
        }

        // TODO 3.双流join
        // TODO 3.1为两条流指定为KV形式
        val infoDStreamWithKey: DStream[(String, OrderInfo)] = infoDStream.map {
            orderInfo => {
                (orderInfo.id, orderInfo)
            }
        }

        val detailDStreamWithKey: DStream[(String, OrderDetail)] = detailDStream.map {
            detailInfo => {
                (detailInfo.order_id, detailInfo)
            }
        }

        // TODO 3.2两条KV流按key进行outerjoin 同一批次中join上的数据为 (join_key,(join_info,join_detail))
        val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = infoDStreamWithKey.fullOuterJoin(detailDStreamWithKey)

        // TODO 3.3 对同一批次的流中每个数据进行处理 先判断info_opt是否为空
        val saleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions { partitionIter => {

            val jedis: Jedis = RedisUtil.getJedisClient()
            implicit val formats = org.json4s.DefaultFormats
            val sales: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()

            for ((id, (info_opt, detail_opt)) <- partitionIter) {
                if (info_opt != None) { // TODO 3.4 info_opt不为空
                    val info: OrderInfo = info_opt.get

                    if (detail_opt != None) { // TODO 3.4.1 此时detail_opt 和 info_opt都不为空 直接join上了 把join的这条直接扔到list中
                        val detail: OrderDetail = detail_opt.get
                        sales += new SaleDetail(info, detail)
                    }

                    // TODO 3.4.2 info把自己的信息写入redis中 1.类型:String 2.Key = order_id:订单id 3.value = 订单json
                    // 采用scala专用的json序列化包 json4s
                    jedis.setex(s"order_info:$id", 60, Serialization.write(info))

                    // TODO 3.4.3 redis缓存中也可能有info_opt的信息 也需要匹配 看是否可以join得上
                    val detailJsonStrings: util.Set[String] = jedis.smembers(s"order_detail:$id")
                    import collection.JavaConversions._
                    for (detailJsonString <- detailJsonStrings) {
                        sales += new SaleDetail(
                            info,
                            JSON.parseObject(detailJsonString, classOf[OrderDetail])
                        )
                    }

                } else { // TODO 3.5 info_opt为空 此时 detail_opt必定不为空

                    val detail: OrderDetail = detail_opt.get

                    // TODO 3.5.1 从缓存中读取info信息 看是否匹配
                    val infoJsonString: String = jedis.get(s"order_info:$id")
                    if (infoJsonString != null && infoJsonString.length > 0) {
                        val info: OrderInfo = JSON.parseObject(infoJsonString, classOf[OrderInfo])
                        sales += new SaleDetail(info, detail)
                    }

                    // TODO 3.5.2 将detail信息写入缓存 1.类型:set 2.key = order_detail:订单id 3.value = set(订单详情json集合)
                    val order_detail_key = "order_detail:" + id
                    jedis.sadd(order_detail_key, Serialization.write(detail))
                    jedis.expire(order_detail_key, 60)

                }
            }
            jedis.close()
            sales.toIterator
        }
        }

        // TODO 4.从Kafka中获取监听user信息变化的流 并写入redis
        val userInfoDStream = RealtimeKafkaUtil.getKafkaStream(ssc,constants.KAFKA_TOPIC_USER)

        userInfoDStream.map{
            record => {
                val userInfoJsonString: String = record.value()
                JSON.parseObject(userInfoJsonString,classOf[UserInfo])
            }
        }.foreachRDD{
            rdd => rdd.foreachPartition{
                usrInfoIter => {
                    val jedis: Jedis = RedisUtil.getJedisClient()
                    implicit val formats=org.json4s.DefaultFormats
                    for(usrInfo <- usrInfoIter){ // 1.类型：哈希 2.key:"user_info" 3.field:用户的id号 4.用户信息json
                        jedis.hset("user_info",usrInfo.id,Serialization.write(usrInfo))
                    }
                    jedis.close()
                }
            }
        }


        // TODO 5.销售详情流与用户流再合并 直接从持久化容器中合并
        val fullSaleDetailDStream: DStream[SaleDetail] = saleDetailDStream.mapPartitions {
            saleDetailIter => {
                val jedis: Jedis = RedisUtil.getJedisClient()
                val saleDetails: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
                for (saleDetail <- saleDetailIter) {
                    val userJsonString: String = jedis.hget("user_info", saleDetail.user_id)
                    val info: UserInfo = JSON.parseObject(userJsonString, classOf[UserInfo])
                    saleDetail.mergeUserInfo(info)
                    saleDetails += saleDetail
                }
                jedis.close()
                saleDetails.toIterator
            }
        }

        val fillSaleDetailDStreamWithKey: DStream[(String, SaleDetail)] = fullSaleDetailDStream.map {
            detail => (detail.order_detail_id, detail)
        }


        // TODO 6.将这个流存储到ES中
        fillSaleDetailDStreamWithKey.foreachRDD{
            rdd => rdd.foreachPartition{
                saleDetailIter => RealtimeESUtil.insertBulk("cat_2019_sale_detail",saleDetailIter.toList)
            }
        }

        ssc.start()
        ssc.awaitTermination()

    }
}

case class OrderDetail(
                              id:String ,
                              order_id: String,
                              sku_name: String,
                              sku_id: String,
                              order_price: String,
                              img_url: String,
                              sku_num: String
                      )

case class SaleDetail(
                             var order_detail_id:String =null,
                             var order_id: String=null,
                             var order_status:String=null,
                             var create_time:String=null,
                             var user_id: String=null,
                             var sku_id: String=null,
                             var user_gender: String=null,
                             var user_age: Int=0,
                             var user_level: String=null,
                             var sku_price: Double=0D,
                             var sku_name: String=null,
                             var dt:String=null)
{
    def this(orderInfo:OrderInfo,orderDetail: OrderDetail) {
        this
        mergeOrderInfo(orderInfo)
        mergeOrderDetail(orderDetail)

    }

    def mergeOrderInfo(orderInfo:OrderInfo): Unit ={
        if(orderInfo!=null){
            this.order_id=orderInfo.id
            this.order_status=orderInfo.order_status
            this.create_time=orderInfo.create_time
            this.dt=orderInfo.create_date
            this.user_id=orderInfo.user_id
        }
    }


    def mergeOrderDetail(orderDetail: OrderDetail): Unit ={
        if(orderDetail!=null){
            this.order_detail_id=orderDetail.id
            this.sku_id=orderDetail.sku_id
            this.sku_name=orderDetail.sku_name
            this.sku_price=orderDetail.order_price.toDouble


        }
    }

    def mergeUserInfo(userInfo: UserInfo): Unit ={
        if(userInfo!=null){
            this.user_id=userInfo.id

            val formattor = new SimpleDateFormat("yyyy-MM-dd")
            val date: util.Date = formattor.parse(userInfo.birthday)
            val curTs: Long = System.currentTimeMillis()
            val  betweenMs= curTs-date.getTime
            val age=betweenMs/1000L/60L/60L/24L/365L

            this.user_age=  age.toInt
            this.user_gender=userInfo.gender
            this.user_level=userInfo.user_level

        }
    }
}

case class UserInfo(id:String ,
                    login_name:String,
                    user_level:String,
                    birthday:String,
                    gender:String) {

}





