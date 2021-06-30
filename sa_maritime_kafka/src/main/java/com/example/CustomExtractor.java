package com.example;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class CustomExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        AISMessage s = (AISMessage) consumerRecord.value();
        //System.out.println("------------");
        //System.out.println(s);
        //ystem.out.println("------------");
        
        return Long.parseLong(s.getTimestamp());
        //return consumerRecord.timestamp();
    }
}
