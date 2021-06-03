package br.com.vithor.ecommerce;

import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

	public static void main(String[] args) {
		
		FraudDetectorService fraudDetectorService = new FraudDetectorService();
		                                                                                               //Esse consumidor, consome Order
		KafkaService<Order> kafkaService = new KafkaService<Order>(FraudDetectorService.class.getSimpleName(),"ECOMMERCE_NEW_ORDER", 
				fraudDetectorService::parse, Order.class, new HashMap<>());
		
		kafkaService.run();
					
	}
	 
	private void parse(ConsumerRecord<String, Order> record) {
		System.out.println("Processing new order, checking for fraud");
		System.out.println(record.key() + " - " + record.value() + "Partition: " + record.partition());
	}

}
