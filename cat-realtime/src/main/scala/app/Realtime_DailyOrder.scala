package app

import com.alibaba.fastjson.JSON
import com.wang930126.cat.common.constants
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.RealtimeKafkaUtil

/**
  * 实时流处理 把Kafka中topic=CAT_ORDER的消息写入到HBASE中
  */
object Realtime_DailyOrder {
    def main(args: Array[String]): Unit = {

        // TODO 1.获取流处理程序的上下文对象
        val sparkconf: SparkConf = new SparkConf().setAppName("daily_order").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(sparkconf,Seconds(5))

        // TODO 2.利用sparkstreaming 和 kafka的对接工具获得输入流 输入流内部封装的是一个个的ConsumerRecord对象
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = RealtimeKafkaUtil.getKafkaStream(ssc,constants.KAFKA_TOPIC_ORDER)

        // TODO 3.将流中封装的ConsumerRecord转为json字符串
        val jsonStringDStream: DStream[String] = inputDStream.map(_.value())

        //{"payment_way":"2",
        // "delivery_address":"dyfgqjazvEKOOCvwVoHs",
        // "consignee":"fmUMbW",
        // "create_time":"2020-07-30 00:42:17",
        // "order_comment":"RggmRySSMiBhhpdxbNMw",
        // "expire_time":"",
        // "order_status":"2",
        // "out_trade_no":"9566382590",
        // "tracking_no":"",
        // "total_amount":"682.0",
        // "user_id":"95",
        // "img_url":"",
        // "province_id":"6",
        // "consignee_tel":"13506184075",
        // "trade_body":"",
        // "id":"380",
        // "parent_order_id":"",
        // "operate_time":"2020-07-30 01:28:41"}

        // TODO 4.将json字符串转为要输出的样例类对象
        // 并且 从create_time字段中提取输出 添加create_date 和 小时create_hour 给电话字段加密
        val orderInfoDStream: DStream[OrderInfo] = jsonStringDStream.map {
            jsonString => {
                val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
                val fields: Array[String] = orderInfo.create_time.split(" ")
                orderInfo.create_date = fields(0)
                orderInfo.create_hour = fields(1).split(":")(0)
                orderInfo.consignee_tel = "*******" + orderInfo.consignee_tel.substring(7, 11)
                orderInfo
            }
        }

        // TODO 5.将这个流存入PhoenixHBase中
        import org.apache.phoenix.spark._
        orderInfoDStream.foreachRDD{
            rdd:RDD[OrderInfo] => {
                println("本批次数据:" + rdd.count())
                rdd.saveToPhoenix(
                    "CAT_ORDER_INFO",
                    Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
                    new Configuration(),
                    Some("spark105,spark106,spark107:2181")
                )
            }
        }

        ssc.start()
        ssc.awaitTermination()

    }
}

case class OrderInfo(id: String,
                     province_id: String,
                     consignee: String,
                     order_comment: String,
                     var consignee_tel: String,
                     order_status: String,
                     payment_way: String,
                     user_id: String,
                     img_url: String,
                     total_amount: Double,
                     expire_time: String,
                     delivery_address: String,
                     create_time: String,
                     operate_time: String,
                     tracking_no: String,
                     parent_order_id: String,
                     out_trade_no: String,
                     trade_body: String,
                     var create_date: String,
                     var create_hour: String
                    )

