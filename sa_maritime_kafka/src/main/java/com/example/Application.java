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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;



public class Application {
    static final String IN_TOPIC = "ais-topic";
    static final String OUT_TOPIC = "output";
    static final String WITHIN_AREA_TOPIC = "within-area";
    static final String CHANGE_HEADING_TOPIC = "change-heading";
    static final String CHANGE_HEADING_FALSE_TOPIC = "change-heading-false";
    static final String TRAWLING_MOVEMENT_TOPIC = "trawling-movement";
    static final String TRAWL_SPEED_TOPIC = "trawl-speed";
    static final String TIME_TRAWL_TOPIC = "time-trawl";
    static final String TEST = "test";
    static final String PATH = "/home/mivia/Desktop/ais_data/trawling_test.csv";
    static final String APP_NAME = "ais-stream";
    static final String BROKER = "localhost:9092";
  
    private static boolean TypeshipCheck(AISMessage v){
        String type = v.getType();
        if ( type.equals("FISHING")){
            return true;
        }
        return false;
        
    }

    static ValueJoiner<String,String,String> windowJoiner =new ValueJoiner<String,String,String>(){
        @Override
        public String apply(String value1, String value2) {
            final String[] value_1_data = value1.split(",");
            final String[] value_2_data = value2.split(",");
            Long time_1_start = Long.parseLong(value_1_data[0]);
            Long time_2_start = Long.parseLong(value_2_data[0]);
            Long time_1_end = Long.parseLong(value_1_data[1]);
            Long time_2_end = Long.parseLong(value_2_data[1]);
                
            Long result_start = Math.max(time_1_start, time_2_start);
            Long result_end = Math.min(time_1_end, time_2_end);
            Long difference = (result_end - result_start)/(60 * 1000);
            return String.format("%s,%s,%s",Long.toString(result_start),Long.toString(result_end),Long.toString(difference));
        };
    };

    public static void main(final String[] args){
        
        try {
            new AISProducer(Application.PATH);
        } catch (CsvValidationException | IOException e) {
            e.printStackTrace();
        }
 
        Properties props = streamConfig();
        Topology stream_Topology = buildTopology(props);
        final KafkaStreams streams = new KafkaStreams(stream_Topology, props);
        
        System.out.println("________________________________");
        System.out.println(stream_Topology.describe());
        System.out.println("________________________________");
        
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static Properties streamConfig(){
        final Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, Application.APP_NAME);
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Application.BROKER);
        props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Set to latest so we analyze only live data (useful for test)
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // disable caching to see session merging
        //props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        
        return props;
    }

    static Topology buildTopology(Properties props){
        
        StreamsBuilder builder = new StreamsBuilder();

        /*
        Adding Store to Topology
        */
        StoreBuilder<KeyValueStore<String,String>> keyValueStoreBuilder =
        Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("heading_store"),
                 Serdes.String(),
                 Serdes.String());
        builder.addStateStore(keyValueStoreBuilder);

        StoreBuilder<KeyValueStore<String,String>> keyValueStoreBuilder_whitinArea =
        Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("whithinArea_store"),
                 Serdes.String(),
                 Serdes.String());
        builder.addStateStore(keyValueStoreBuilder_whitinArea);

        StoreBuilder<KeyValueStore<String,String>> keyValueStoreBuilder_trawlSpeed =
        Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("trawlSpeed_store"),
                 Serdes.String(),
                 Serdes.String());
        builder.addStateStore(keyValueStoreBuilder_trawlSpeed);

      
        //
        final KStream<String, AISMessage> source = builder.stream(Application.IN_TOPIC, 
            Consumed.with(Serdes.String(),
            AISSerders.AISMessage()).withTimestampExtractor(new CustomExtractor()).withName(Application.IN_TOPIC));
        
        //FILTER FISHING VESSEL
        final KStream<String, AISMessage> sourceFishing = source.filter((k,v) -> Application.TypeshipCheck(v),Named.as("TypeshipCheck"));
        // sourceFishing.to(Application.TEST, Produced.with(Serdes.String(), AISSerders.AISMessage()));   
      
        //STREAM FILTER VESSEL WHITHIN AREA
        final KStream<String, String> whitinArea = sourceFishing.transform(new TransformerSupplier<String,AISMessage,KeyValue<String,String>>(){
            @Override
            public Transformer<String, AISMessage, KeyValue<String,String>> get() {
                return new WindowTransfWithinArea();
            }
            
        }, Named.as("processor_whithinArea") ,"whithinArea_store");
        whitinArea.to(Application.WITHIN_AREA_TOPIC, Produced.with(Serdes.String(), Serdes.String()));


        final KStream<String, String> headingChange = sourceFishing.transform(new TransformerSupplier<String,AISMessage,KeyValue<String,String>>(){
            @Override
            public Transformer<String, AISMessage, KeyValue<String,String>> get() {
                return new WindowTransfChangeHeading();
            }
            
        }, Named.as("processor_heading") ,"heading_store");
        headingChange.to(Application.CHANGE_HEADING_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, String> trawlingMovement = whitinArea.join(headingChange,
        windowJoiner, 
        JoinWindows.of(Duration.ofMillis(1000)),
        StreamJoined.with(Serdes.String(),Serdes.String(),Serdes.String()).withName("Trawling-movement-join")
        );
        
        //trawlingMovement.to(Application.TRAWLING_MOVEMENT_TOPIC);
        //TrawlingMovement.filter((k,v) -> v != null)
        KStream<String, String> trawlingMovement_filter = trawlingMovement.filter(new Predicate<String,String>(){
            @Override
            public boolean test(String key, String value) {
                final String[] data = value.split(",");
                Long startTimestamp = Long.parseLong(data[0]);
                Long endTimestamp = Long.parseLong(data[1]);
                if(endTimestamp-startTimestamp>= Duration.ofMinutes(10).toMillis())
                    return true;
                return false;
            }
            
        });

        trawlingMovement_filter.to(Application.TRAWLING_MOVEMENT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        
    
        //WINDOW FOR EVALUATE TRAWLSPEED 
        final KStream<String, String> trawlSpeed = sourceFishing.transform(new TransformerSupplier<String,AISMessage,KeyValue<String,String>>(){
            @Override
            public Transformer<String, AISMessage, KeyValue<String,String>> get() {
                return new WindowTransfTrawlSpeed();
            }
            
        }, Named.as("processor_speed") ,"trawlSpeed_store");

        //FILTER NULL
        trawlSpeed.filter((k,v) -> v != null)
        .to(Application.TRAWL_SPEED_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, String> trawling = trawlSpeed.join(trawlingMovement_filter,windowJoiner, 
        JoinWindows.of(Duration.ofMillis(1000)),
        StreamJoined.with(Serdes.String(),Serdes.String(),Serdes.String()).withName("Trawling-join")  
        );
        trawling.to(Application.OUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build(props);

    }

    
}
