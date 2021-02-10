package com.zmboo.lotemall.api.config;


import com.zmboo.lotemall.api.common.Sender;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public Map<String,Object> producerConfigs() {
        Map<String,Object> props = new HashMap<String,Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka:9092");//idea调试本地docker容器内的kafka 改为localhost
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG,"0");
        return props;
    }

    @Bean
    public ProducerFactory<String,String> producerFactory() {
        return  new DefaultKafkaProducerFactory<String,String>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String,String> kafkaTemplate() {
        return new KafkaTemplate<String, String>(producerFactory());
    }

    @Bean
    public Sender sender() {
        return new Sender();
    }
}
