package com.example;

import java.io.IOException;
import java.time.Duration;
/* import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Locale;
import java.util.TimeZone; */
import java.util.Properties;


import com.opencsv.exceptions.CsvValidationException;

import org.apache.kafka.common.protocol.types.Field.Str;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;



public class AISProcessorDemo {
    static final String IN_TOPIC = "ais-topic";
    static final String OUT_TOPIC = "output";
    static final String WITHIN_AREA_TOPIC = "within-area";
    static final String CHANGE_HEADING_TOPIC = "change-heading";
    static final String CHANGE_HEADING_FALSE_TOPIC = "change-heading-false";
    static final String TRAWLING_MOVEMENT_TOPIC = "trawling-movement";
    static final String TRAWL_SPEED_TOPIC = "trawl-speed";
    static final String TIME_TRAWL_TOPIC = "time-trawl";
    static final String TEST = "test";
    static final String PATH = "/home/mivia/Desktop/ais_data/ais_data_types.csv";
    static final String APP_NAME = "ais-stream";
    static final String BROKER = "localhost:9092";
    static final float SPEED_MIN = 1.0f;
    static final float SPEED_MAX = 9.0f;
    static final FishingArea FishingArea = new FishingArea();
   
    



    private static boolean TypeshipCheck(AISMessage v){
        String type = v.getType();
        if ( type.equals("FISHING")){
            return true;
        }
        return false;
        
    }

    private static boolean TrawlSpeedCheck(AISMessage v, float speed_min, float speed_max){

        Float speed = Float.parseFloat(v.getSpeed());

        if (speed >= speed_min && speed <= speed_max){
            return true;
        } 
        return false;
    }

    private static boolean headingChangeCheck(AISMessage v){

        String annotation = v.getAnnotation();
        if (annotation.charAt(2) == '1'){
            return true;
        }
        return false;
    }

    private static boolean TimeCheck(String v,Duration timeDuration){

        final String[] data = v.split(",");
        Long diff = Long.parseLong(data[2]);
        if (diff >= timeDuration.toMillis()){
            return true;
        }
        return false;
    }

    /* private final static DateTimeFormatter timeFormatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM)
    .withLocale(Locale.ITALY)
    .withZone(ZoneId.ofOffset("UTC", ZoneOffset.ofHours(0))); */
    
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

        Topology topology = new Topology();

        
        
        

        final KStream<String, AISMessage> source = builder.stream(AISProcessorDemo.IN_TOPIC, Consumed.with(Serdes.String(), AISSerders.AISMessage()).withTimestampExtractor(new CustomExtractor()));
        final KStream<String, AISMessage> sourceFishing = source.filter((k,v) -> AISProcessorDemo.TypeshipCheck(v));
        //sourceFishing.to(AISProcessorDemo.TEST, Produced.with(Serdes.String(), AISSerders.AISMessage()));

        



        final KStream<String, AISMessage> whitinArea = sourceFishing.filter((k,v) -> FishingArea.is_in_FishingArea(v));
        whitinArea.to(AISProcessorDemo.WITHIN_AREA_TOPIC, Produced.with(Serdes.String(), AISSerders.AISMessage()));

        final KStream<String, AISMessage> TrawlSpeed = sourceFishing.filter((k,v) -> TrawlSpeedCheck(v, AISProcessorDemo.SPEED_MIN, AISProcessorDemo.SPEED_MAX));
        TrawlSpeed.to(AISProcessorDemo.TRAWL_SPEED_TOPIC, Produced.with(Serdes.String(), AISSerders.AISMessage()));

        final KStream<String, AISMessage> headingChange = sourceFishing.filter((k,v) -> headingChangeCheck(v));
        headingChange.to(AISProcessorDemo.CHANGE_HEADING_TOPIC, Produced.with(Serdes.String(), AISSerders.AISMessage()));


        topology.addSource("source",AISProcessorDemo.CHANGE_HEADING_TOPIC)
            .addProcessor("ProcessorHeading", new AISProcessorSupplier(), "source")
            .addSink("result",AISProcessorDemo.TEST,"ProcessorHeading");
        
        
        // create store
        StoreBuilder<KeyValueStore<String,String>> keyValueStoreBuilder =
        Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("Heading_sequence"),
            Serdes.String(),
            Serdes.String());
        // add store
        builder.addStateStore(keyValueStoreBuilder);

        topology.addProcessor(AISProcessorDemo.TEST, new AISProcessorSupplier(), AISProcessorDemo.CHANGE_HEADING_TOPIC);

       
        final KStream<String, AISMessage> headingChange_false = sourceFishing.filter((k,v) -> !headingChangeCheck(v));
        headingChange_false.to(AISProcessorDemo.CHANGE_HEADING_FALSE_TOPIC, Produced.with(Serdes.String(), AISSerders.AISMessage()));

            
        final KStream<String, String> headingChange_window =builder.stream(AISProcessorDemo.CHANGE_HEADING_TOPIC, Consumed.with(Serdes.String(), AISSerders.AISMessage()))
            .groupByKey()
            .windowedBy(SessionWindows.with(Duration.ofMinutes(10)).grace(Duration.ofMinutes(1)))
            .count()
            //.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .map((windowedKey, count) ->  {
                //String start = timeFormatter.format(windowedKey.window().startTime());
                //String end = timeFormatter.format(windowedKey.window().endTime());
                Long start = windowedKey.window().startTime().toEpochMilli();
                Long end = windowedKey.window().endTime().toEpochMilli();
                Long diff = end - start;
                String sessionInfo = String.format("%s,%s,%s,%s", Long.toString(start), Long.toString(end),Long.toString(diff), Long.toString(count));
                return KeyValue.pair(windowedKey.key(), sessionInfo);
            });
        headingChange_window.to(AISProcessorDemo.TEST, Produced.with(Serdes.String(), Serdes.String()));

        
        final KStream<String, String> headingChange_window_false =builder.stream(AISProcessorDemo.CHANGE_HEADING_FALSE_TOPIC, Consumed.with(Serdes.String(), AISSerders.AISMessage()))
            .groupByKey()
            .windowedBy(SessionWindows.with(Duration.ofMinutes(10)).grace(Duration.ofMinutes(1)))
            .count()
            //.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .map((windowedKey, count) ->  {
                //String start = timeFormatter.format(windowedKey.window().startTime());
                //String end = timeFormatter.format(windowedKey.window().endTime());
                Long start = windowedKey.window().startTime().toEpochMilli();
                Long end = windowedKey.window().endTime().toEpochMilli();
                Long diff = end - start;
                String sessionInfo = String.format("%s,%s,%s,%s", Long.toString(start), Long.toString(end),Long.toString(diff), Long.toString(count));
                return KeyValue.pair(windowedKey.key(), sessionInfo);
            });
        headingChange_window_false.to(AISProcessorDemo.TRAWLING_MOVEMENT_TOPIC,Produced.with(Serdes.String(),Serdes.String()));

        KStream<String, String> trawling = headingChange_window.join(headingChange_window_false,
            new ValueJoiner<String,String,String>(){
                @Override
                public String apply(String value1, String value2) {
                    
                    final String[] value_1_data = value1.split(",");
                    final String[] value_2_data = value2.split(",");
                    Long time_1_start = Long.parseLong(value_1_data[0]);
                    Long time_1_end = Long.parseLong(value_1_data[1]);
                    Long time_2_end = Long.parseLong(value_2_data[1]);
                    
                    Long result_end = Math.min(time_1_end, time_2_end);
                    return String.format("%s,%s",Long.toString(time_1_start),Long.toString(result_end));
                };
            }, 
            JoinWindows.of(Duration.ofMillis(1000)),
            StreamJoined.with(
                Serdes.String(),
                Serdes.String(),
                Serdes.String())  
            );
            trawling.to(AISProcessorDemo.OUT_TOPIC,Produced.with(Serdes.String(), Serdes.String()));
/*

        KStream<String, AISMessage> trawlingMovement = whitinArea.join(headingChange,
        (leftValue, rightValue) -> rightValue, 
        JoinWindows.of(Duration.ofMillis(1000)),
        StreamJoined.with(
            Serdes.String(),
            AISSerders.AISMessage(),
            AISSerders.AISMessage())  
        );
        //trawlingMovement.to(AISProcessorDemo.TRAWLING_MOVEMENT_TOPIC);

        KStream<String, AISMessage> trawling = TrawlSpeed.join(trawlingMovement,
        (leftValue, rightValue) -> rightValue, 
        JoinWindows.of(Duration.ofMillis(1000)),
        StreamJoined.with(
            Serdes.String(),
            AISSerders.AISMessage(),
            AISSerders.AISMessage())  
        ); 
        trawling.to(AISProcessorDemo.TRAWLING_MOVEMENT_TOPIC,Produced.with(Serdes.String(), AISSerders.AISMessage()));

        /* final KStream<Windowed<String>, String> TrawlingWindowTEST =builder.stream(AISProcessorDemo.TRAWLING_MOVEMENT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.with(Duration.ofMinutes(10)).grace(Duration.ofMinutes(1)))
            .reduce(new Reducer<String>() {
                
                @Override
                public String apply(String savedValue, String currValue) {
                  return currValue;
                }
              })
            .toStream();
 

        final KStream<String, String> TrawlingWindow =builder.stream(AISProcessorDemo.TRAWLING_MOVEMENT_TOPIC, Consumed.with(Serdes.String(), AISSerders.AISMessage()))
            .groupByKey()
            .windowedBy(SessionWindows.with(Duration.ofMinutes(10)).grace(Duration.ofMinutes(1)))
            .count()
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .map((windowedKey, count) ->  {
                //String start = timeFormatter.format(windowedKey.window().startTime());
                //String end = timeFormatter.format(windowedKey.window().endTime());
                Long start = windowedKey.window().startTime().toEpochMilli();
                Long end = windowedKey.window().endTime().toEpochMilli();
                Long diff = end - start;
                String sessionInfo = String.format("%s,%s,%s,%s", Long.toString(start), Long.toString(end),Long.toString(diff), Long.toString(count));
                return KeyValue.pair(windowedKey.key(), sessionInfo);
            });

            //.to(AISProcessorDemo.TEST, Produced.with(Serdes.String(), Serdes.String()));


        final KStream<String, String> TimeTrawlingCheck= TrawlingWindow.filter((k,v) -> TimeCheck(v,Duration.ofMinutes(10)));
        TimeTrawlingCheck.to(AISProcessorDemo.TIME_TRAWL_TOPIC,Produced.with(Serdes.String(), Serdes.String()));

         */
        
        /* final KStream<String, String> TrawlingSpeedWindow =builder.stream(AISProcessorDemo.TRAWL_SPEED_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
        .groupByKey()
        .windowedBy(SessionWindows.with(Duration.ofHours(1)).grace(Duration.ofMinutes(10)))
        .count()
        .suppress(Suppressed.untilTimeLimit(Duration.ofHours(1), Suppressed.BufferConfig.unbounded()))
        //.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((windowedKey, count) ->  {
            //String start = timeFormatter.format(windowedKey.window().startTime());
            //String end = timeFormatter.format(windowedKey.window().endTime());
            Long start = windowedKey.window().startTime().toEpochMilli();
            Long end = windowedKey.window().endTime().toEpochMilli();
            Long diff = end - start;
            String sessionInfo = String.format("%s,%s,%s,%s", Long.toString(start), Long.toString(end),Long.toString(diff), Long.toString(count));
            return KeyValue.pair(windowedKey.key(), sessionInfo);
        }); 
        final KStream<String, String> TrawlingSpeedWindowCheck= TrawlingSpeedWindow.filter((k,v) -> TimeCheck(v,Duration.ofHours(1)));
        TrawlingSpeedWindowCheck.to(AISProcessorDemo.TEST,Produced.with(Serdes.String(), Serdes.String()));
        */

        

        /* KStream<String, String> trawling = TimeTrawlingMovementCheck.join(TrawlingSpeedWindowCheck,
        new ValueJoiner<String,String,String>(){
            @Override
            public String apply(String value1, String value2) {
                final String[] value_1_data = value1.split(",");
                final String[] value_2_data = value2.split(",");
                Long time_1_start = Long.parseLong(value_1_data[0]);
                Long time_2_start = Long.parseLong(value_2_data[0]);
                Long time_1_end = Long.parseLong(value_1_data[1]);
                Long time_2_end = Long.parseLong(value_2_data[1]);
                Long result_start = Math.max(time_1_start, time_2_start);
                Long result_end = Math.max(time_1_end, time_2_end);
                return String.format("%s,%s,%s",Long.toString(result_start),Long.toString(result_end),Long.toString(result_end-result_start));
                
            };
        }, 
        JoinWindows.of(Duration.ofMillis(1000)),
        StreamJoined.with(
            Serdes.String(),
            Serdes.String(),
            Serdes.String())  
        );
        trawling.to(AISProcessorDemo.OUT_TOPIC,Produced.with(Serdes.String(), Serdes.String()));

         */
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    
}
