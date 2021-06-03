package br.com.vithor.ecommerce;

import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class EmailService {

	public static void main(String[] args) {
		
		EmailService emailService = new EmailService();
		
		KafkaService<String> kafkaService = new KafkaService<String>(EmailService.class.getSimpleName(),"ECOMMERCE_SEND_EMAIL", 
				emailService::parse, String.class, new HashMap<String, String>() {{ put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); }} );
		
		kafkaService.run();
	}
	
	private void parse(ConsumerRecord<String, String> record) {
		System.out.println("Sending Email");
		System.out.println(record.key() + " - " + record.value() + "Partition: " + record.partition());
	}
	

}
