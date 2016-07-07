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

public class PayStatBolt implements IRichBolt {

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
		long orderId = input.getLong(0);
		long createdTime = input.getLong(1);
		double amount = input.getDouble(2);
		while(true){
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
				preKey += createdTime;
				//判断hashmap里是否存在
				Object oldAmount = this.tairOperator.get(preKey);
				if(oldAmount !=null){
					amount += Double.valueOf(oldAmount.toString());
				}
				this.tairOperator.write(preKey, RaceUtils.round(amount,2));
				System.out.println("Result: ["+preKey+","+ RaceUtils.round(amount,2)+"]");
				break;
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		this.collector.ack(input);
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
