package com.zmboo.lotemall.api.controller;

import com.alibaba.fastjson.JSON;
import com.zmboo.lotemall.api.common.KafkaProducerCommon;
import com.zmboo.lotemall.api.common.Sender;
import com.zmboo.lotemall.api.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Random;

@RestController
@RequestMapping("/order")
public class OrderController {

    @Autowired
    private Sender sender;

    //@RequestMapping(value = "/order/{id}", method = RequestMethod.GET)
    @GetMapping("/{id}")
    public String selectOrder(@PathVariable Long id) {
        //kafkaProducerCommon.producerKafka();
        return "select success:"+id.toString();
    }

    @PostMapping
    public String insertOrder(@RequestBody Order order) {
        sender.producerKafka(order);
        return "{\"code\":0,\"message\":\"insert success\"}";
    }

}

