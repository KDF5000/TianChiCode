package com.alibaba.middleware.race.model;

public class DataTuple {
	public static int MQ_TMALL_ORDER = 0;
	public static int MQ_TAOBAO_ORDER = 1;
	public static int MQ_PAY = 2;
	
	private int type; //0—天猫 1-淘宝订单，2-支付
	private long orderId; //订单id
	private OrderMessage orderMessage = null; //从mq里取出的消息，可以根据type转换为相应实体
	private PaymentMessage payMessage = null;
	protected long emitMs; 
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	
	public OrderMessage getOrderMessage() {
		return orderMessage;
	}
	public void setOrderMessage(OrderMessage orderMessage) {
		this.orderMessage = orderMessage;
	}
	public PaymentMessage getPayMessage() {
		return payMessage;
	}
	public void setPayMessage(PaymentMessage payMessage) {
		this.payMessage = payMessage;
	}
	public long getEmitMs() {
		return emitMs;
	}

	public void updateEmitMs() {
		this.emitMs = System.currentTimeMillis();
	}
	public long getOrderId() {
		return orderId;
	}
	public void setOrderId(long orderId) {
		this.orderId = orderId;
	}
}
