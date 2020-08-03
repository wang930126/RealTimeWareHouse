package app

import com.alibaba.fastjson.JSON
import com.wang930126.cat.common.constants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import util.RealtimeKafkaUtil
import util.RealtimeESUtil
import java.util

import scala.util.control.Breaks._

/**
  * 需求：将满足所有条的如下的用户查找出来 并输出到ES中
  * 1.5分钟内使用同一个mid访问并且使用不同的uid
  * 2.不浏览任何的商品 并且超过三次领取购物券
  * 3.1分钟内只向ES写入一次
  */
object RealTime_Alert {
    def main(args: Array[String]): Unit = {

        //日志数据源格式
        //upload{
        // "area":"beijing",
        // "uid":"456",
        // "itemid":43,
        // "npgid":50,
        // "evid":"coupon",
        // "os":"andriod",
        // "pgid":6,
        // "appid":"gmall2019",
        // "mid":"mid_156",
        // "type":"event"}

        // TODO 1.设置spark的配置文件 给出上下文对象
        val sparkConf: SparkConf = new SparkConf().setAppName("Real-timeAlert").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

        // TODO 2.利用工具类从kafka中获取输入流 流中RDD对象为一个个ConsumerRecord对象组成的集合
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = RealtimeKafkaUtil.getKafkaStream(ssc,
            constants.KAFKA_TOPIC_EVENT)

        // TODO 3.将ConsumerRecord对象映射为样例类对象
        val eventDStream: DStream[EventInfo] = inputDStream.map {
            record: ConsumerRecord[String, String] => {
                val jsonString: String = record.value()
                val event: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
                event
            }
        }

        eventDStream.foreachRDD{
            rdd => println("1." + rdd.count())
        }

        // TODO 4.对事件流进行开窗 size=5min slide=10s
        val eventWindowDStream: DStream[EventInfo] = eventDStream.window(Minutes(5),Seconds(10))

        // TODO 5.对每个窗口中的所有的rdd进行分组 按设备mid号进行分组
        val groupByMidDStream: DStream[(String, Iterable[EventInfo])] = eventWindowDStream.mapPartitions {
            iter: Iterator[EventInfo] => {
                var infoes: Array[(String, EventInfo)] = Array[(String, EventInfo)]()
                iter.foreach {
                    elem => {
                        infoes = infoes.+:((elem.mid, elem))
                    }
                }
                infoes.toIterator
            }
        }.groupByKey()

        // TODO 6.对分组后的流进行处理 对迭代器进行遍历 看是否符合预警的条件
        val beforeAlertStream: DStream[(Boolean, CouponAlertInfo)] = groupByMidDStream.map {
            case (mid: String, iter: Iterable[EventInfo]) => {

                val couponUidSet = new util.HashSet[String]()
                //存储访问优惠券的uid
                val itemIdSet = new util.HashSet[String]()
                //存储领取优惠券的商品id
                val eventList = new util.ArrayList[String]()
                //存储所有的事件
                var flag = true

                breakable {
                    for (elem <- iter) {
                        eventList.add(elem.evid)
                        // TODO 6.1 如果事件是优惠券类型
                        if (elem.evid == "coupon") {
                            couponUidSet.add(elem.uid)
                            itemIdSet.add(elem.itemid)
                        } else if (elem.`type` == "clickItem") { //TODO 6.2如果有点击事件 flag置为false
                            flag = false
                            break()
                        }
                    }
                }

                // 如果满足条件返回(true,CouponAlert)
                (couponUidSet.size() > 3 && flag,
                        CouponAlertInfo(
                            mid,
                            couponUidSet,
                            itemIdSet,
                            eventList,
                            System.currentTimeMillis()
                        )
                )
            }
        }


        // TODO 7.为流添加一个主键用于幂等写入的实现
        val midTsToInfoDStream: DStream[(String, CouponAlertInfo)] = beforeAlertStream.filter(_._1).map {
            case (flag: Boolean, info: CouponAlertInfo) => {
                val ts = (info.ts / 1000L / 60L).toString
                (s"${info.mid}_$ts", info)
            }
        }

        // TODO 8.对每个RDD执行foreachFunc操作
        midTsToInfoDStream.foreachRDD{
            rdd:RDD[(String,CouponAlertInfo)] => {
                rdd.foreachPartition(
                    iter => RealtimeESUtil.insertBulk(constants.ES_INDEX_COUPON_ALERT,iter.toList)
                )
            }
        }

        ssc.start()
        ssc.awaitTermination()

    }
}

case class EventInfo(mid:String,
                     uid:String,
                     appid:String,
                     area:String,
                     os:String,
                     ch:String,
                     `type`:String,
                     evid:String ,
                     pgid:String ,
                     npgid:String ,
                     itemid:String,
                     var logDate:String,
                     var logHour:String,
                     var ts:Long
                    )

case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)


