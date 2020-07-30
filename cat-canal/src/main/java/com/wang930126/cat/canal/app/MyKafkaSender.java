package com.wang930126.cat.canal.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaSender {

    public static KafkaProducer<String,String> kafkaProducer = null;

    static{
        Properties pro = new Properties();
        pro.put("bootstrap.servers","spark105:9092,spark106:9092,spark107:9092");
        pro.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        pro.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer(pro);
    }

    public static void send(String topic, String message) {

        kafkaProducer.send(new ProducerRecord<String,String>(topic,message));

    }

}
