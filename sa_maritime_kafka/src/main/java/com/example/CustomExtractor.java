package com.example;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class CustomExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        String s = (String) consumerRecord.value();
        System.out.println("------------");
        System.out.println(s);
        System.out.println("------------");
        ArrayList<String> data = new ArrayList<>(Arrays.asList(s.split(","))); 
        return Long.parseLong(data.get(6));
        //return consumerRecord.timestamp();
    }
}
