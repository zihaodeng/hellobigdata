package com.zmboo.lotemall.api;

import com.zmboo.lotemall.api.controller.OrderController;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableKafka
public class LotemallWebApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(LotemallWebApiApplication.class, args);
    }

}
