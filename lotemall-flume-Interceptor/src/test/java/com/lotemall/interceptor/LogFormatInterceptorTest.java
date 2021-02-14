package com.lotemall.interceptor;

import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializerFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class LogFormatInterceptorTest {

    @Test
    public void initialize() {
    }

    @Test
    public void intercept() {
        String body ="2020-06-30 22:07:20.581 [http-nio-8080-exec-4] INFO  com.zmboo.lotemall.controller.StartController - content:{\"country\":\"china\",\"area\":\"shenzhen\"}";

        if(body.indexOf("content:{")<=0) return;

        String date =body.substring(0,10);
        String country = body.substring(body.indexOf("country")+10,body.indexOf("area")-3);
        String area =body.substring(body.indexOf("area")+7,body.length()-2);

        System.out.println(String.format("%s,%s,%s", date,country,area));
        System.out.println(String.format("%s,%s,%s", date,country,area).getBytes());
    }

    @Test
    public void testIntercept() {
    }

    @Test
    public void close() {
    }
}