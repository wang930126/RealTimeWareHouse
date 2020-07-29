package util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable


object RealtimeKafkaUtil {

    private val properties:Properties = PropertiesUtil.load("config.properties")
    private val brokerList: String = properties.getProperty("kafka.broker.list")
    private val map: mutable.HashMap[String, Object] = mutable.HashMap[String,Object]()

    //集群id 反序列化键值的方法 消费者组设定
    map.put("bootstrap.servers",brokerList)
    map.put("key.deserializer",classOf[StringDeserializer])
    map.put("value.deserializer",classOf[StringDeserializer])
    map.put("group.id","cat_consumer_group")

    //kafka自动重置偏移量为最新的偏移量 从最新的offset开始消费
    map.put("auto.offset.reset","latest")

    //自动提交打开与否关系到是否要手动维护偏移量 如果是true这个消费者的偏移量会在后台自动提交 但是如果Spark处理失败了容易丢失数据
    map.put("enable.auto.commit",true:java.lang.Boolean)

    /**
      *
      * @param ssc sparkstreaming上下文对象
      * @param topic spark订阅的上游的kafka主题
      * @return 返回一个InputDStream对象 用于消费消息
      */
    def getKafkaStream(ssc:StreamingContext,topic:String) = {

        val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](List(topic), map)
        )
        inputDStream
    }

}
