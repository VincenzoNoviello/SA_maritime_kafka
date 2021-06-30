package com.example;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;


public class AISSerders extends Serdes.WrapperSerde<AISMessage>{
    
    public AISSerders(Serializer<com.example.AISMessage> serializer,
            Deserializer<com.example.AISMessage> deserializer) {
        super(serializer, deserializer);
        
    }

    static public final class AISMessageSerde
            extends Serdes.WrapperSerde<AISMessage> {
        public AISMessageSerde() {
            super(new AISMessageSerializer<>(),
                    new AISMessageDeserializer<>(AISMessage.class));
        }
    }

    public static Serde<AISMessage> AISMessage() {
        return new AISSerders.AISMessageSerde();
    }

}
