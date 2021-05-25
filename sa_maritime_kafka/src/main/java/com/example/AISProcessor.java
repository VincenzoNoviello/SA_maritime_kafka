package com.example;

import java.time.Duration;


import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class AISProcessor implements Processor<String, String, String, String>{

    /**
     *
     */
    private ProcessorContext<String,String> context;
    private KeyValueStore<String, Integer> kvStore;


    @Override
    public void init(final ProcessorContext<String,String> context) {
        this.context = context;
        this.kvStore = context.getStateStore("WithinAreaCount");
        this.context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
            try (final KeyValueIterator<String, Integer> iter = kvStore.all()) {
                System.out.println("----------- " + timestamp + " ----------- ");

                while (iter.hasNext()) {
                    final KeyValue<String, Integer> entry = iter.next();

                    System.out.println("[" + entry.key + ":  " + entry.value + "]");
                    
                    context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
                }
            }
        });
    }

    @Override
    public void close() {
        
    }

    @Override
    public void process(Record<String, String> recordInput) {
        final String[] data = recordInput.value().split(",");
        String id = data[2];
        Float longitude = Float.parseFloat(data[4]);
        Float latitude = Float.parseFloat(data[3]);

        Integer oldValue = kvStore.get(id);
        //System.out.println(Arrays.toString(data));
        //if (latitude < 48.335456 && latitude> 48.144979 && longitude > -4.879198 && longitude < -4.645699){
        if (latitude < 49 && latitude> 48 && longitude > -5 && longitude < -4){
            this.kvStore.put(id, 1);
            System.err.println("STO METTENDO NEL KVSTORE");
            //context.forward(new Record(id, 1, System.currentTimeMillis()));
            //long timestamp = System.currentTimeMillis();
            //context.forward(new Record(id, '1',timestamp));
            //context.commit();
        } else {
            if (oldValue != null){
                kvStore.delete(id);
            }
        }
    }
    
}
