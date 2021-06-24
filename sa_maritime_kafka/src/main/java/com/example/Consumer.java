package com.example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;

public class Consumer implements Runnable {

    private String brokers;
    private String groupId;
    private String topicName;

    public Consumer(String brokers, String groupId, String topicName){
        this.brokers = brokers;
        this.groupId = groupId;
        this.topicName = topicName;
    }

    private int consume(String brokers, String groupId, String topicName) {
        // Create a consumer
        KafkaConsumer<String, String> consumer;
        // Configure the consumer
        Properties properties = new Properties();
        // Point it to the brokers
        properties.setProperty("bootstrap.servers", brokers);
        // Set the consumer group (all consumers must belong to a group).
        properties.setProperty("group.id", groupId);
        // Set how to serialize key/value pairs
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // When a group is first created, it has no offset stored to start reading from. This tells it to start
        // with the earliest record in the stream.
        properties.setProperty("auto.offset.reset","earliest");
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomExtractor.class.getName());

        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        consumer = new KafkaConsumer<>(properties);

        // Subscribe to the 'test' topic
        consumer.subscribe(Arrays.asList(topicName));

        // Loop until ctrl + c
        int count = 0;
        while(true) {
            // Poll for records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2L));
            // Did we get any?
            if (records.count() == 0) {
                // timeout/nothing to read
            } else {
                // Yes, loop over records
                for(ConsumerRecord<String, String> record: records) {
                    // Display record and count
                    count += 1;
                    System.out.println(record.toString());
                    System.out.println(record.timestamp());
                    System.out.println("\n");
                }
            }
        }
    }

    @Override
    public void run() {
        this.consume(this.brokers, this.groupId, this.topicName);
        
    }
}