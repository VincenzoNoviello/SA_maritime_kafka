package com.example;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import com.opencsv.exceptions.CsvValidationException;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Transformer;


public class AISProcessorDemo {

    static final String IN_TOPIC = "ais-topic";
    static final String OUT_TOPIC = "output";
    static final String WITHIN_AREA_TOPIC = "within-area";
    static final String CHANGE_HEADING_TOPIC = "change-heading";
    static final String TRAWLING_MOVEMENT_TOPIC = "trawling-movement";
    static final String TRAWL_SPEED_TOPIC = "trawl-speed";
    static final String PATH = "/home/mivia/Desktop/ais_data/ais_data.csv";
    static final String APP_NAME = "ais-stream1";
    static final String BROKER = "localhost:9092";
    static final float MAX_LAT = 49.335456f;
    static final float MIN_LAT = 47.044979f;
    static final float MAX_LOG = -5.879198f;
    static final float MIN_LOG = -3.545699f;
    static final float SPEED_MIN = 1.0f;
    static final float SPEED_MAX = 9.0f;
    
    private static boolean withinAreaCheck(String v, float max_lat, float min_lat, float max_log, float min_log){

        final String[] data = v.split(",");
        Float longitude = Float.parseFloat(data[4]);
        Float latitude = Float.parseFloat(data[3]);

        if (latitude < max_lat && latitude > min_lat && longitude > max_log && longitude < min_log){
            return true;
        } 
        return false;
    }

    private static boolean TrawlSpeedCheck(String v, float speed_min, float speed_max){

        final String[] data = v.split(",");
        Float speed = Float.parseFloat(data[5]);

        if (speed >= speed_min && speed <= speed_max){
            return true;
        } 
        return false;
    }

    private static boolean headingChangeCheck(String v){

        final String[] data = v.split(",");
        String annotation = data[0];
        if (annotation.charAt(2) == '1'){
            return true;
        }
        return false;
    }

    private final static DateTimeFormatter timeFormatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM)
    .withLocale(Locale.ITALY)
    .withZone(ZoneId.ofOffset("UTC", ZoneOffset.ofHours(0)));
    
    public static void main(final String[] args){
        final Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, AISProcessorDemo.APP_NAME);
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AISProcessorDemo.BROKER);
        props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomExtractor.class.getName());
        try {
            new AISProducer(AISProcessorDemo.PATH);
        } catch (CsvValidationException | IOException e) {
            e.printStackTrace();
        }

        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> source = builder.stream(AISProcessorDemo.IN_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new CustomExtractor()));
        //source.groupByKey().windowedBy(SessionWindows.with(Duration.ofSeconds(2)));
        Consumer consumer = new Consumer(AISProcessorDemo.BROKER, AISProcessorDemo.APP_NAME, "within-area");
        
        final KStream<String, String> whitinArea = source.filter((k,v) -> withinAreaCheck(v, AISProcessorDemo.MAX_LAT, AISProcessorDemo.MIN_LAT, AISProcessorDemo.MAX_LOG,AISProcessorDemo.MIN_LOG));
        whitinArea.to(AISProcessorDemo.WITHIN_AREA_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> TrawlSpeed = source.filter((k,v) -> TrawlSpeedCheck(v, AISProcessorDemo.SPEED_MIN, AISProcessorDemo.SPEED_MAX));
        TrawlSpeed.to(AISProcessorDemo.TRAWL_SPEED_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> headingChange = source.filter((k,v) -> headingChangeCheck(v));
        headingChange.to(AISProcessorDemo.CHANGE_HEADING_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    
        /*
        KStream<String, String> trawlingMovement = whitinArea.join(headingChange,
        (leftValue, rightValue) -> rightValue, 
        JoinWindows.of(Duration.ofSeconds(300)),
        StreamJoined.with(
            Serdes.String(),
            Serdes.String(),
            Serdes.String())  
        ); 
        trawlingMovement.to(AISProcessorDemo.OUT_TOPIC);
        */

         
        builder.stream(AISProcessorDemo.TRAWL_SPEED_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.with(Duration.ofSeconds(10)))
            .count()
            .toStream()
            .map((windowedKey, count) ->  {
                String start = timeFormatter.format(windowedKey.window().startTime());
                String end = timeFormatter.format(windowedKey.window().endTime());
                //Long start = windowedKey.window().startTime().toEpochMilli();
                //Long end = windowedKey.window().endTime().toEpochMilli();
                String sessionInfo = String.format("Session info started: %s ended: %s with count %s", start, end, count);
                return KeyValue.pair(windowedKey.key(), sessionInfo);
            })
            .to(AISProcessorDemo.OUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    
}
