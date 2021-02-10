package com.zmboo.lotemall.api.common;

import com.alibaba.fastjson.JSON;
import com.zmboo.lotemall.api.entity.Covid19;
import com.zmboo.lotemall.api.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Random;

public class Sender {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    private static Random rand = new Random();

    public void producerKafka(Order order){
        order.setPayTime(String.valueOf(new Timestamp(System.currentTimeMillis()+ rand.nextInt(100))));//EventTime
        //flink-kafka2mysql
        kafkaTemplate.send("order-log", JSON.toJSONString(order));
    }

    public void producerKafkaByES(Covid19 entity){
        //flink-kafka2es
        kafkaTemplate.send("covid19count-log", JSON.toJSONString(entity));
    }
}
