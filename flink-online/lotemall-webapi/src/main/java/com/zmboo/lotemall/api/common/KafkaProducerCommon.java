package com.zmboo.lotemall.api.common;

import com.alibaba.fastjson.JSON;
import com.zmboo.lotemall.api.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Random;

@Deprecated
@Component
public class KafkaProducerCommon {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    private static Random rand = new Random();

    public void producerKafka(){
        Order order =new Order();
        order.setOrderId(99L);
        order.setGoodsId(2);
        order.setUserId(99);
        order.setAddress("深圳市");
        order.setPayTime(String.valueOf(new Timestamp(System.currentTimeMillis()+ rand.nextInt(100))));//EventTime
        order.setAmount(BigDecimal.valueOf(999.99));
        kafkaTemplate.send("order-log", JSON.toJSONString(order));
    }

    public void producerKafka(Order order){
        order.setPayTime(String.valueOf(new Timestamp(System.currentTimeMillis()+ rand.nextInt(100))));//EventTime
        kafkaTemplate.send("order-log", JSON.toJSONString(order));
    }

    private String getSecondTimestamp(String millis){
        return millis.substring(0,millis.indexOf("."));
    }
}
