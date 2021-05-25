package com.example;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.opencsv.exceptions.CsvValidationException;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;


public class AISProcessorDemo {

    public static void main(final String[] args){
        final String PATH = "C:/Users/vince/Documents/GitHub/SituationAwareness_vessel/ais_brest_synopses.csv";
        final Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "ais-stream");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try {
            new AISProducer(PATH);
        } catch (CsvValidationException | IOException e) {
            //e.printStackTrace();
            System.err.println("ERRORE CSV");
        }
        final Topology builder = new Topology();
        builder.addSource("Source", "stream-plaintext-input")
        .addProcessor("ProcessWithinArea", new AISProcessorWithinArea(), "Source")

        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("WithinAreaCount"),
            Serdes.String(),
            Serdes.Integer()),
            "ProcessWithinArea")
        
        .addSink("Sink", "streams-linesplit-output", "ProcessWithinArea");

        System.out.println(builder.describe());
        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
                
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
    
}
