package br.com.vithor.ecommerce;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

//Classe para deserializar especificamente um tipo (Podendo ser string, Order, qualquer tipo)
//Usadas nos properties do consumer (KafkaService)
//É necessaria em casos que eu quero deserializar objeto para objeto (envio Order e quero receber Order)
public class GsonDeserializer<T> implements Deserializer<T> {
	
	public static final String TYPE_CONFIG = "br.com.vithor.ecommerce.type_config"; //Qualquer string
	private Gson gson = new GsonBuilder().create();
	private Class<T> type;
	
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		String typeName = String.valueOf(configs.get(TYPE_CONFIG));
		try {
			this.type = (Class<T>) Class.forName(typeName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		return gson.fromJson(new String(data), type);
	}

}
