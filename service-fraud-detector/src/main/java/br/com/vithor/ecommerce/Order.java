package br.com.vithor.ecommerce;

import java.math.BigDecimal;

public class Order {
	
	private String userId, orderId;
	
	private BigDecimal amount;

	public Order(String userId, String orderId, BigDecimal value) {
		super();
		this.userId = userId;
		this.orderId = orderId;
		this.amount = value;
	}
	

}
