package com.example;


import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;


import org.apache.kafka.streams.state.KeyValueStore;

public class WindowResultTrawling implements Transformer<String, String,KeyValue<String,String>>{

    private ProcessorContext context;
    private KeyValueStore<String, String> kvStore;
    // private KeyValueStore<String, Integer> kvStore;
    
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        System.out.println("__________________sto inizializzando");

        this.kvStore = context.getStateStore("trawling_result_store");
        this.kvStore.flush();
        
        
    }
    

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        
        if (this.kvStore.get(key)==null){

                kvStore.put(key,value);
                
        } else {
            String kvstore_string =kvStore.get(key);
            final String[] kvStoreData = kvstore_string.split(",");
            final String[] value_data = value.split(",");
            String time_start = value_data[0];
            String time_start_store = kvStoreData[0];
            System.out.println(time_start+","+time_start_store);
            if(time_start.equals(time_start_store)){
                kvStore.put(key,value);
                System.out.println("sto aggiornando");
            }else{
                kvStore.put(key,value);
                System.out.println("sto pubblicando");
                return new KeyValue<String,String>(key,kvstore_string);
            }
            //context.forward(id, new_value);
        }
    return null;
        
    }

    @Override
    public void close() {
        this.kvStore.flush();
        this.kvStore.close();
        context.timestamp();
    }

    
}

    

