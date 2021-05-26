package com.example;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class ProcessorWithinArea implements ProcessorSupplier<String, String, String, String>{
    
    @Override
    public Processor<String, String, String, String> get() {
        return new AISProcessorWithinArea();
    }
    
}
