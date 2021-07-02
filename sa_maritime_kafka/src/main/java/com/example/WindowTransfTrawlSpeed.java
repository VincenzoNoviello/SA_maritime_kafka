package com.example;


import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;


import org.apache.kafka.streams.state.KeyValueStore;

public class WindowTransfTrawlSpeed implements Transformer<String, AISMessage,KeyValue<String,String>>{

    private ProcessorContext context;
    private KeyValueStore<String, String> kvStore;
    final FishingArea FishingArea = new FishingArea();
    // private KeyValueStore<String, Integer> kvStore;
    static final float SPEED_MIN = 1.0f;
    static final float SPEED_MAX = 9.0f;


    private static boolean TrawlSpeedCheck(AISMessage v, float speed_min, float speed_max){

        Float speed = Float.parseFloat(v.getSpeed());

        if (speed >= speed_min && speed <= speed_max){
            return true;
        } 
        return false;
    }
    
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        System.out.println("__________________sto inizializzando");

        this.kvStore = context.getStateStore("trawlSpeed_store");
        this.kvStore.flush();
        
    }
    

    @Override
    public KeyValue<String, String> transform(String key, AISMessage value) {
        
        String id = value.getId();
        String new_value;
        if (this.kvStore.get(id)==null){
            if (TrawlSpeedCheck(value, WindowTransfTrawlSpeed.SPEED_MIN, WindowTransfTrawlSpeed.SPEED_MAX)){
                new_value = String.format(value.getTimestamp() +','+ value.getTimestamp());
                //System.out.println(id+','+new_value+','+ "NUOVA FINESTRA");
                kvStore.put(id, new_value);
                return new KeyValue<String,String>(id,new_value);
            }
                
        } else {
            final String[] kvStoreData = kvStore.get(id).split(",");
            //System.out.println(id+" "+Arrays.toString(kvStoreData));
            Long startTimestamp = Long.parseLong(kvStoreData[0]);
            //Long precTimestamp = Long.parseLong(kvStoreData[1]);
            Long currTimestamp = Long.parseLong(value.getTimestamp());
            Long difference = (currTimestamp - startTimestamp)/(60 * 1000);

            if (TrawlSpeedCheck(value, WindowTransfTrawlSpeed.SPEED_MIN, WindowTransfTrawlSpeed.SPEED_MAX)){
                
                    String new_value_insert =String.format(startTimestamp.toString() + ','+ currTimestamp.toString());
                    kvStore.put(id,new_value_insert);
                    //System.out.println(id+','+new_value_insert+','+ "MENO_10_min");
                    kvStore.put(id,new_value_insert);
                    return new KeyValue<String,String>(id,new_value_insert+","+Long.toString(difference));
                    //System.out.println(id+','+new_value+','+ "PIU_10_min");
            }else{
                kvStore.delete(id);
               // System.out.println(id+','+ "FALSO");
                
                return new KeyValue<String,String>(id,String.format(startTimestamp.toString() + ','+ currTimestamp.toString()+","+Long.toString(difference)));
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

    

