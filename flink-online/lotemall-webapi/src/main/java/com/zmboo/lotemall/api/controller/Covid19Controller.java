package com.zmboo.lotemall.api.controller;

import com.zmboo.lotemall.api.common.Sender;
import com.zmboo.lotemall.api.entity.Covid19;
import com.zmboo.lotemall.api.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/covid19")
public class Covid19Controller {
    @Autowired
    private Sender sender;

    //@RequestMapping(value = "/order/{id}", method = RequestMethod.GET)
    @GetMapping("/{id}")
    public String selectOrder(@PathVariable Long id) {
        //kafkaProducerCommon.producerKafka();
        return "select success:"+id.toString();
    }

    @PostMapping
    public String insertOrder(@RequestBody Covid19 entity) {
        sender.producerKafkaByES(entity);
        return "{\"code\":0,\"message\":\"insert success\"}";
    }
}
