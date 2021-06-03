package br.com.vithor.ecommerce;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonSerializer<T> implements Serializer<T>{
	
	private Gson gson = new GsonBuilder().create();

	@Override
	public byte[] serialize(String topic, T data) {
		//o data pode ser tanto chave quanto valor, de qualquer tipo
		return gson.toJson(data).getBytes();
	}

}
