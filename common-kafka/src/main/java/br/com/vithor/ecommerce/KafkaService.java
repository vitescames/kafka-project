package br.com.vithor.ecommerce;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaService<T> {
	
	private final KafkaConsumer<String, T> consumer;
	private final ConsumerFunction<T> function;

	public KafkaService(String groupId, String topic, ConsumerFunction<T> function, Class<T> classType, HashMap<String, String> props) {
		
		this.function = function;
		
		consumer = new KafkaConsumer<>(properties(classType, groupId, props));
		
		consumer.subscribe(Collections.singletonList(topic));
					
	}
	
	//Construtor para consumidor de mais de um topico (LogService)
	public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> function, Class<T> classType, HashMap<String, String> props) {
		
		this.function = function;
		
		consumer = new KafkaConsumer<>(properties(classType, groupId, props));
		
		consumer.subscribe(topic);
					
	}

	public void run() {
		
		while(true) {
			
			ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
			
			if(!records.isEmpty()) {
				System.out.println("Encontrei " + records.count() + " mensagens");
				for (ConsumerRecord<String, T> consumerRecord : records) {
					function.consume(consumerRecord);
				}
			}
		}
		
	}
	
	private Properties properties(Class<T> classType, String groupId, Map<String, String> overrideProps) {
		Properties properties = new Properties();
		//Servidor que esta rodando o kafka
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		//No consumidor, agora temos que deserializar de bytes para string
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		//Para cada consumer, precisamos de um grupo
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		//Precisamos passar aqui, qual o tipo de retorno, senão da erro de parse
		properties.setProperty(GsonDeserializer.TYPE_CONFIG, classType.getName());
		
		//Para propridades extras, como no caso do LogService que teria que ter um deserializador diferente do GsonDeserializer, usamos o Map
		properties.putAll(overrideProps);
		return properties;
	}

}
