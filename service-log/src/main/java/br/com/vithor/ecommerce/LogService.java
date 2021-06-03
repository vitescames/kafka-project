package br.com.vithor.ecommerce;

import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class LogService {

	public static void main(String[] args) {
		
		LogService logService = new LogService();
		
		KafkaService<String> kafkaService = new KafkaService<String>(LogService.class.getSimpleName(), Pattern.compile("ECOMMERCE.*"), 
				logService::parse, String.class, new HashMap<String, String>() {{ put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); }} );
		                                                                           //aqui passamos um hashmap com uma propriedade que será sobrescrita nos properties
		
		kafkaService.run();

	}
	
	private void parse(ConsumerRecord<String, String> record) {
		System.out.println("LOG: " + record.topic());
		System.out.println(record.key() + " - " + record.value() + "Partition: " + record.partition());
	}

}
