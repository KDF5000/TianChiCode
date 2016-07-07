package com.alibaba.middleware.race.jstorm.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.processing.RoundEnvironment;

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
	
	//debug
	private HashMap<String, Double> result = new HashMap<String, Double>();
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
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
			this.tairOperator.write(preKey, RaceUtils.round(newAmount,2));
			System.out.println("Result: ["+preKey+","+ RaceUtils.round(newAmount,2)+"]");
		}else{
			//重新放入消息队列
		}
		this.collector.ack(input);
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
