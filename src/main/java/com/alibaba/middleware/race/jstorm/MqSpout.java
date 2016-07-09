package com.alibaba.middleware.race.jstorm;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;



import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.DataTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MqSpout implements IRichSpout, MessageListenerConcurrently {
	private static final long serialVersionUID = 1L;
	protected transient DefaultMQPushConsumer consumer; //消费者
	protected transient LinkedBlockingDeque<DataTuple> sendingQueue; // 消息队列
	protected Map conf;
	
	protected SpoutOutputCollector collector;
	/**
	 * 
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.conf = conf;
		this.sendingQueue = new LinkedBlockingDeque<DataTuple>();
		try {
			this.consumer = MqConsumerFactory.mkInstance(this);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		if(this.consumer != null){
			this.consumer.shutdown();
		}
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		if(this.consumer != null){
			this.consumer.resume();
		}
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		if(this.consumer != null){
			this.consumer.suspend();
		}
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		DataTuple dataTuple = null;
		try {
			dataTuple = this.sendingQueue.take();
		} catch (Exception e) {
			// TODO: handle exception
		}
		if(dataTuple == null){
			return;
		}

		if(dataTuple.getType() == DataTuple.MQ_PAY){
			PaymentMessage paymentMessage = dataTuple.getPayMessage();

			long timestamp = RaceUtils.getMinuteTime(paymentMessage.getCreateTime());
			this.collector.emit("order_pay", new Values(DataTuple.MQ_PAY, paymentMessage.getOrderId(),paymentMessage.getPayAmount(),timestamp));
			this.collector.emit("ratio_out", new Values(paymentMessage.getPayPlatform(), paymentMessage.getPayAmount(),timestamp));
		}else{
			OrderMessage orderMessage = dataTuple.getOrderMessage();
			long timestamp = RaceUtils.getMinuteTime(orderMessage.getCreateTime());
			this.collector.emit("order_pay", new Values(dataTuple.getType(),orderMessage.getOrderId(), orderMessage.getTotalPrice(), timestamp));
		}
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("order_pay", new Fields("type", "orderId","amount","createdTime"));
		declarer.declareStream("ratio_out", new Fields("platform", "amount", "createdTime"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	
	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
		// TODO Auto-generated method stub
		for (MessageExt msg : msgs) {
			DataTuple dataTuple = new DataTuple();
            byte [] body = msg.getBody();
            if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                //Info: 生产者停止生成数据, 并不意味着马上结束
                System.out.println("Got the end signal");
                continue;
            }
            String topic = msg.getTopic();
            if(topic.equals(RaceConfig.MqPayTopic)){
            	dataTuple.setType(DataTuple.MQ_PAY);
            	PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
            	dataTuple.setPayMessage(paymentMessage);
            	dataTuple.setOrderId(paymentMessage.getOrderId());
//            	System.out.println("Receive Pay Messge: ["+paymentMessage.getOrderId()+","+dataTuple.getType()+"]");
            }else if(topic.equals(RaceConfig.MqTmallTradeTopic)){
            	dataTuple.setType(DataTuple.MQ_TMALL_ORDER);
            	OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
            	dataTuple.setOrderMessage(orderMessage);
//            	System.out.println("Receive Tmall Order Messge: ["+orderMessage.getOrderId()+","+dataTuple.getType()+"]");
            	dataTuple.setOrderId(orderMessage.getOrderId());
            }else if( topic.equals(RaceConfig.MqTaobaoTradeTopic)){
            	dataTuple.setType(DataTuple.MQ_TAOBAO_ORDER);
            	OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
            	dataTuple.setOrderMessage(orderMessage);
            	dataTuple.setOrderId(orderMessage.getOrderId());
//            	System.out.println("Receive Taobao Order Messge: ["+orderMessage.getOrderId()+","+dataTuple.getType()+"]");
            }else{
            	System.out.println("Unknow message!!!!");
            	continue;
            }
            this.sendingQueue.offer(dataTuple);
        }
		return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	}

}
