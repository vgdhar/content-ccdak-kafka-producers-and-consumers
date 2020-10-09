package com.linuxacademy.ccdak.clients;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerMain {

    public static void main(String[] args) {
        //System.out.println("Hello, World!");
    	Properties props= new Properties();
    	props.put("bootstrap.servers", "localhost:9092");
    	props.put("group.id", "group1");
    	props.put("enable.auto.commit","false");
    	props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    	props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    	
    	KafkaConsumer<String, String> consumer= new KafkaConsumer<>(props);
    	
    	consumer.subscribe(Arrays.asList("test_topic1","test_topic2"));
	    while(true)
	    {
	    	ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(100));
	    	for(ConsumerRecord<String, String> record : records)
	    	{
	    		System.out.println("Key= " +record.key() 
	    		+",value= "+record.value()
	    		+",topic= "+record.topic()
	    		+",partition= "+record.partition()
	    		+",offset= "+record.offset());
	    	}
	    	consumer.commitSync();
	    }
	    
    }

}
