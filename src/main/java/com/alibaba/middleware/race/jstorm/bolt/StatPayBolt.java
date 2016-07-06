package com.alibaba.middleware.race.jstorm.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.thrift.server.THsHaServer;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.DataTuple;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class StatPayBolt implements IRichBolt {

	private OutputCollector collector;
	private TairOperatorImpl tairOperator = null;
	private DefaultMQProducer producer;
	
	//debug
	private HashMap<String, Double> result = new HashMap<String, Double>();
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		producer = new DefaultMQProducer("please_rename_unique_group_name");

        //在本地搭建好broker后,记得指定nameServer的地址
        producer.setNamesrvAddr(RaceConfig.MqNameServer);

        try {
			producer.start();
		} catch (MQClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		PaymentMessage paymentMessage = (PaymentMessage)input.getValue(0);
		long orderId = paymentMessage.getOrderId();
		//查询tair
		Object orderType = tairOperator.get(orderId);
		if(orderType != null){
			String preKey = "";
			System.out.println("从Tair获取orderType:"+orderId+","+orderType.toString());
			if(Integer.valueOf(orderType.toString()) == DataTuple.MQ_TAOBAO_ORDER){
				//
				preKey += RaceConfig.prex_taobao+RaceConfig.TeamCode+"_";
			}else{
				preKey += RaceConfig.prex_tmall+RaceConfig.TeamCode+"_";
			}
			long timestamp = paymentMessage.getCreateTime() / 1000/60 * 60;
			preKey += timestamp;
			//判断hashmap里是否存在
			Object amount = this.tairOperator.get(preKey);
			double newAmount = paymentMessage.getPayAmount();
			if(amount !=null){
				newAmount += Double.valueOf(amount.toString());
			}
			this.tairOperator.write(preKey, newAmount);
			System.out.println("Result: ["+preKey+","+ newAmount+"]");
//			if(this.result.containsKey(preKey)){
//				double amount = this.result.get(preKey) + paymentMessage.getPayAmount();
//				this.result.put(preKey, amount);
//			}else{
//				this.result.put(preKey, paymentMessage.getPayAmount());
//			}

		}else{
			//重新放入消息队列
//			try {
//				this.storeMessage(paymentMessage);
//			} catch (MQClientException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (RemotingException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
//		printResult();
		this.collector.ack(input);
	}
	/**
	 * 重新存储没有找到订单id的消息
	 * @param msg
	 * @throws MQClientException
	 * @throws RemotingException
	 * @throws InterruptedException
	 */
	private void storeMessage(PaymentMessage msg) throws MQClientException, RemotingException, InterruptedException{
		if(msg == null){
			return;
		}
		
        final Message messageToBroker = new Message(RaceConfig.MqPayTopic, RaceUtils.writeKryoObject(msg));
        this.producer.send(messageToBroker, new SendCallback() {
            public void onSuccess(SendResult sendResult) {
                System.out.println("重新放回RocketMQ:"+messageToBroker);
            }
            public void onException(Throwable throwable) {
                throwable.printStackTrace();
            }
        });
	}
	/**
	 * 打印结果
	 */
	private void printResult(){
		Iterator<Map.Entry<String, Double>> entries = this.result.entrySet().iterator();
		System.out.print("Result:");
		while (entries.hasNext()) {
		    Map.Entry<String, Double> entry = entries.next();
		    System.out.print("[" + entry.getKey() + "," + entry.getValue()+"],");
		}
		System.out.println();
	}
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
