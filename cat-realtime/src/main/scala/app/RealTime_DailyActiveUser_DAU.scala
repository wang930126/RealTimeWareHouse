package app

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.wang930126.cat.common.constants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import util.{RealtimeKafkaUtil, RedisUtil}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast

/**
  * 实现对kafka中的数据进行消费 统计每日的活跃用户数目(分时？)
  */
object RealTime_DailyActiveUser_DAU {
    def main(args: Array[String]): Unit = {

        /*
        	消费kafka中的数据。
        	利用redis过滤当日已经计入的日活设备。
        	把每批次新增的当日日活信息保存到HBASE或ES中。
        	从ES中查询出数据，发布成数据接口，通可视化化工程调用。
        */

        // TODO 1.获取sparkconf配置文件 设置集群模式
        val sparkConf: SparkConf = new SparkConf().setAppName("dau-analysis").setMaster("local[*]")

        // TODO 2.获取SparkStreamingContext对象
        val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

        // TODO 3.通过工具类获取Kafka Source
        val inputStream: InputDStream[ConsumerRecord[String, String]] = RealtimeKafkaUtil.getKafkaStream(ssc,constants.KAFKA_TOPIC_STARTUP)

        // TODO 4.对流进行处理 把其中每个consumerRecord传递进来的json字符串转换成样例类对象
        val startUpLogStream: DStream[StartUpLog] = inputStream.map(consumerRecord => {
            val jsonString: String = consumerRecord.value()
            val log: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
            val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
            val date: Date = new Date(log.ts)
            val dateString: String = sdf.format(date)
            log.logDate = dateString.split(" ")(0)
            log.logHour = dateString.split(" ")(1)
            log
        })

        // TODO 5.根据redis中缓存的结果 对同一批次数据进行过滤 只保留之前未访问的访问用户

        // 在Driver中执行 执行一次
        val filteredDStream: DStream[StartUpLog] = startUpLogStream.transform(
            rdd => {
                println("过滤前，本批次数据量：" + rdd.count())
                // 在Driver中执行 有多少批次个5000ms 执行多少次

                // TODO 5.1 同一批次的数据建立一次连接 去redis中取出所有的mid放在Driver的一个集合中 并将之广播
                val conn = RedisUtil.getJedisClient() //获取连接
                val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date()) //获取当前的时间字符
                val dauMidSet: util.Set[String] = conn.smembers("dau:" + dateString)
                conn.close()
                val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)

                // TODO 5.2 Exectuor端 就可以直接从广播变量中去判断mid是否存在
                val filterRDD: RDD[StartUpLog] = rdd.filter( log =>{
                    val set = dauMidBC.value
                    !set.contains(log.mid)
                })

                println("过滤后，本批次数据量：" + filterRDD.count())
                filterRDD
            }
        )


        // TODO 6.同一批次中也可能存在重复的 对同一批次重复的进行去重
        val groupByMidDStream: DStream[(String, Iterable[StartUpLog])] = filteredDStream.map(
            log => (log.mid, log)
        ).groupByKey()

        val distinctStream: DStream[StartUpLog] = groupByMidDStream.map(
                {
                    case (mid: String, iter: Iterable[StartUpLog]) => {
                        iter.toIterator.next()
                    }
                }
        )


        // TODO 7.把每个RDD中的用户id写入redis中 用于去重 存储所有mid
        // TODO 7.1 redis 存储的 一、key ~ dau:2020-07-28 二、value ~ set()
        distinctStream.foreachRDD(
            // TODO 7.2 使用foreachPartition避免频繁创建连接
            rdd => rdd.foreachPartition{
                    iter:Iterator[StartUpLog] => {
                        // TODO 5.3 建立连接池
                        val conn: Jedis = RedisUtil.getJedisClient()
                        for (elem <- iter) {
                            conn.sadd("dau:" + elem.logDate,elem.mid)
                        }
                        // 归还连接conn
                        conn.close()
                    }
            }
        )

        // TODO 8.把去重后的数据写入Phonenix-HBase中
        import org.apache.phoenix.spark._
        distinctStream.foreachRDD({
            rdd:RDD[StartUpLog] => {
                rdd.saveToPhoenix(
                    "CAT2019_DAU",// 表名
                    Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),// 列名
                    new Configuration(),// 写入配置
                    Some("spark105,spark106,spark107:2181")// 写入的zk
                )
            }
        })

        ssc.start()
        ssc.awaitTermination()

    }
}

case class StartUpLog(mid:String,uid:String,appid:String,area:String,
                      os:String,ch:String,logType:String,vs:String,
                      var logDate:String,var logHour:String,var ts:Long)
