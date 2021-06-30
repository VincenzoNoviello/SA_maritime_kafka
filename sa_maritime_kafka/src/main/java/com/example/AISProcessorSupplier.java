package com.example;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class AISProcessorSupplier implements ProcessorSupplier<String, AISMessage, String, String>{
    
    @Override
    public Processor<String, AISMessage, String, String> get() {
        return new AISProcessor();
    }
    
}