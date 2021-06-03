package br.com.vithor.ecommerce;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

//O T se refere ao Generics, dessa forma usamos o mesmo dispatcher para diferentes tipos de valor
public class KafkaDispatcher<T> {
	
	private final KafkaProducer<String, T> producer;
	
	public KafkaDispatcher() {

		this.producer = new KafkaProducer<>(properties());
		
	}
	
	private static Properties properties() {
		Properties properties = new Properties();
		//Onde esta rodando o Kafka pra eu conseguir conectar o produtor?
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		
		//Tanto a chave quanto o valor vao ser serializados para bytes
		//Para chaves e valores String usamos o StringSerializer, mas para outros tipos temos que customizar nosso serializador
		//Com o GsonSerializer, convertemos o dado para json string, que por sua vez é covertido para bytes
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
		return properties;
	}

	//O tipo do parâmetro value é definido no instanciamento desse dispatcher (ex: KafkaDispatcher<Order> ou KafkaDispatcher<String>)
	public void send(String topic, String key, T value) throws InterruptedException, ExecutionException {
		
		//A mensagem, registro, vai ser mandada pra que topico?
		ProducerRecord<String, T> record = new ProducerRecord<>(topic, key, value);
		
		Callback callback = (data, ex) -> {
			if(ex != null) {
				ex.printStackTrace();
			} else {
				System.out.println("Success! " + data.topic() + ":::" + data.partition() + "/" + data.offset() + "/" + data.timestamp());
			}
		};
		
		//Com o get, a aplicação espera o callback de retorno do envio da mensagem.
		producer.send(record, callback).get();
		
	}

}
