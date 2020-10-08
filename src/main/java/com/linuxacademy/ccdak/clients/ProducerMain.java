package com.linuxacademy.ccdak.clients;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerMain {

    public static void main(String[] args) {
        //System.out.println("Hello, World!");
    	
    	Properties props= new Properties();
    	props.put("bootstrap.servers", "localhost:9092");
    	props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
    	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	props.put("acks", "all");
    	Producer<String, String> producer= new KafkaProducer<>(props);
    	for (int i=0 ; i<100;i++)
    	{
    		int partition=0;
    		if(i>49) { partition=1;}
    	ProducerRecord<String, String> record = new ProducerRecord<String, String>("test_count", partition, "count", Integer.toString(i));
    	
    	producer.send(record, (RecordMetadata metadata, Exception e ) ->
    	{
    		if(e !=null)
    		{
    			System.out.println("Error in publishing to the kafka topic" + e.getMessage());
    		}
    		else
    		{
    			System.out.println("Published Message" + "Key :"+ record.key() + 
    									"Value :"+ record.value()+
    									"Topic :" + metadata.topic()+
    									"Partition :"+metadata.partition()+
    									"Offset :" +metadata.offset());
    		}
    	});
    	
    	}
    	producer.close();
}
}
