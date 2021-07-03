package com.example;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.streams.StreamsConfig;

public class AISProducer extends Thread {

    private CSVReader reader;
    private Properties props;
    private KafkaProducer<String, AISMessage> producer;

    public AISProducer(String path) throws IOException, CsvValidationException {
        this.reader = new CSVReader(new FileReader(path));
        this.props = new Properties();
        this.props.put(StreamsConfig.APPLICATION_ID_CONFIG, Application.APP_NAME);
        this.props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomExtractor.class.getName());
        this.props.put("bootstrap.servers",Application.BROKER);
        this.props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        this.props.put("value.serializer","com.example.AISMessageSerializer");
        this.props.put("value.deserializer","com.example.AISMessageDeserializer");

        this.producer = new KafkaProducer<>(this.props);
        System.out.println(this.reader);
         
        this.reader.readNext(); // salta prima riga perchè ci sono i nomi delle colonne
        super.start();
    }

    private AISMessage read() throws IOException, CsvValidationException {
        String s[] = this.reader.readNextSilently();
        AISMessage message;
        //System.out.println(s);
        
        message = new AISMessage(s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]);
        //message = new AISMessage(s[3], s[2], s[0], s[1], s[4], s[5], s[6], s[7]);
        
        return message;
    }


    @Override
    public void run() {
        while (true){
            //i++;
            try {

                AISMessage message = this.read();
                ProducerRecord<String, AISMessage> data_to_publish = new ProducerRecord<>(Application.IN_TOPIC, 0, Long.parseLong(message.getTimestamp()), message.getId(), message);
                producer.send(data_to_publish);
                
                Long timestamp=Long.parseLong(message.getTimestamp());
                
                AISMessage message_next = this.read();
                while(message_next != null){
                    
                    Long timestamp_next=Long.parseLong(message_next.getTimestamp());
                    data_to_publish = new ProducerRecord<>(Application.IN_TOPIC, 0, Long.parseLong(message_next.getTimestamp()), message_next.getId(), message_next);
                    if(timestamp_next.equals(timestamp))
                    {
                        producer.send(data_to_publish);
                    }
                    else{
                        break;
                    }
                    message_next = this.read();
                }
                Thread.sleep(1000);
                //System.out.println("2 SECONDI");
                producer.send(data_to_publish);
                //final ProducerRecord<String, String> data_to_publish = new ProducerRecord<>(AISProcessorDemo.IN_TOPIC, data.get(2), data_read);
                /*
                CustomExtractor extractor = new CustomExtractor();
                ConsumerRecord r = new ConsumerRecord(AISProcessorDemo.IN_TOPIC, 0, 0L, data.get(2), data_read);
                extractor.extract(r, 1624547605020L);
                */
                
                

            } catch (CsvValidationException e) {
                System.out.println(e);
            } catch (IOException e) {
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
    }
}
