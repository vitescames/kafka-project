package br.com.vithor.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {		
		
		KafkaDispatcher<Order> dispatcher = new KafkaDispatcher<>();
		
		String userId = UUID.randomUUID().toString();
		String orderId = UUID.randomUUID().toString();
		BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
		
		Order order = new Order(userId, orderId, amount);
		
		String email = "Thank you! We are processing your order";
		
		dispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
		
		//Precisamos de um novo dispatcher, que aceite String, para o envio de mensagem de email
		
		KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();
		
		emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
		
		
	}	

}
