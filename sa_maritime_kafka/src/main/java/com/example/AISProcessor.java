package com.example;


import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import org.apache.kafka.streams.state.KeyValueStore;

public class AISProcessor implements Processor<String, AISMessage, String, String>{

    private ProcessorContext<String,String> context;
    private KeyValueStore<String, String> kvStore;
    // private KeyValueStore<String, Integer> kvStore;

    @Override
    public void init(final ProcessorContext<String,String> context) {
        this.context = context;
        this.kvStore = context.getStateStore("Timestamp");
    } 
    

    @Override
    public void process(Record<String, AISMessage> recordInput) {
        
        AISMessage message = (AISMessage)recordInput.value();
        String annotation = message.getAnnotation();
        String id = message.getId();

        if (this.kvStore.get(id)==null){
            if (annotation.charAt(2) == '1')
                kvStore.put(id, message.getTimestamp());
        } else {
            final String[] kvStoreData = kvStore.get(id).split(",");

            Long precTimestamp = Long.parseLong(kvStoreData[0]);
            Long currTimestamp = Long.parseLong(message.getTimestamp());
            if (annotation.charAt(2) == '1'){
                kvStore.put(id, String.format(precTimestamp.toString() + ','+ currTimestamp.toString()));
                Record<String,String> to_forward = recordInput.withValue(precTimestamp.toString() + ','+ currTimestamp.toString());
                context.forward(to_forward);
            } else {
                kvStore.delete(id);
                Record<String,String> to_forward = recordInput.withValue(precTimestamp.toString() + ','+ currTimestamp.toString());
                context.forward(to_forward);
            }

            
        }
        
    }
    
}

    

