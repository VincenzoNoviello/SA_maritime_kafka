package com.example;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.streams.StreamsConfig;

public class AISProducer extends Thread {

    private CSVReader reader;
    private Properties props;
    private KafkaProducer<String, String> producer;

    public AISProducer(String path) throws IOException, CsvValidationException {
        this.reader = new CSVReader(new FileReader(path));
        this.props = new Properties();
        this.props.put(StreamsConfig.APPLICATION_ID_CONFIG, AISProcessorDemo.APP_NAME);

        props.put("bootstrap.servers",AISProcessorDemo.BROKER);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(this.props);
        System.out.println(this.reader);
         
        this.reader.readNext(); // salta prima riga perchè ci sono i nomi delle colonne
        //this.builder = new StreamsBuilder();
        super.start();
    }

    private String read() throws IOException, CsvValidationException {
        String s[] = this.reader.readNext();
        AISMessage message = new AISMessage(s[0], s[1], s[2], s[3], s[4], s[5], s[6]);
        return message.toString();
    }

    @Override
    public void run() {
        //int i = 0;
        while (true){
            //i++;
            try {
                String data_read = this.read();
                ArrayList<String> data = new ArrayList<String>(Arrays.asList(data_read.split(",")));
                final ProducerRecord<String, String> data_to_publish = new ProducerRecord<>(AISProcessorDemo.IN_TOPIC, data.get(2), data_read);
                producer.send(data_to_publish);
                Thread.sleep(2000);
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
