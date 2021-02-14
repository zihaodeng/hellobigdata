package com.lotemall.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LogFormatInterceptor implements Interceptor {
    public void initialize() {

    }

    public Event intercept(Event event) {
        /**
         * 实现自己对event的逻辑处理，最终返回event
         */
        if (event == null) return null;

        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());

        if (body.indexOf("content:{") <= 0) return null;

        String date = body.substring(0, 10);
        String country = body.substring(body.indexOf("country") + 10, body.indexOf("area") - 3);
        String area = body.substring(body.indexOf("area") + 7, body.length() - 2);

        event.setBody(String.format("%s,%s,%s", date, country, area).getBytes());
        return event;
    }

    public List<Event> intercept(List<Event> list) {
        List<Event> lst = new ArrayList<Event>();
        Iterator<Event> it = list.iterator();
        while (it.hasNext()) {
            Event event = it.next();
            Event eventSingle = intercept(event);
            if (eventSingle != null) {
                lst.add(eventSingle);
            }
        }
        return lst;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new LogFormatInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
