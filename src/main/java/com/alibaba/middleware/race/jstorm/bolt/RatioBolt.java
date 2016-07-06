package com.alibaba.middleware.race.jstorm.bolt;

import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class RatioBolt implements IRichBolt{
	private OutputCollector collector;
	private TairOperatorImpl tairOperator = null;
	
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
		long timestamp = paymentMessage.getCreateTime() / 1000/60 * 60;
		//把pc和无线的交易额写到tair，然后计算比值
		int platform = paymentMessage.getPayPlatform();
		if(platform == 0){//PC
			//PC
			Object total = this.tairOperator.get("PC_total");
			double newValue = 0;
			if(total != null){
				newValue = Double.valueOf(total.toString()) + paymentMessage.getPayAmount();
			}else{
				newValue = paymentMessage.getPayAmount();
			}
			Object anotherTotal = this.tairOperator.get("Mobile_total");
			double mobileAmount = 0;
			if(anotherTotal != null){
				mobileAmount = Double.valueOf(anotherTotal.toString());
			}
			this.tairOperator.write("PC_total", newValue);
			this.tairOperator.write(RaceConfig.prex_ratio+RaceConfig.TeamCode+"_"+timestamp, newValue>0 ? mobileAmount/newValue : 0);
			System.out.println("Ratio Result: "+ (newValue>0 ? mobileAmount/newValue : 0));
		}else{//Mobile
			Object total = this.tairOperator.get("Mobile_total");
			double newValue = 0;
			if(total != null){
				newValue = Double.valueOf(total.toString()) + paymentMessage.getPayAmount();
			}else{
				newValue = paymentMessage.getPayAmount();
			}
			Object anotherTotal = this.tairOperator.get("PC_total");
			double pcAmount = 0;
			if(anotherTotal != null){
				pcAmount = Double.valueOf(anotherTotal.toString());
			}
			this.tairOperator.write("Mobile_total", newValue);
			this.tairOperator.write(RaceConfig.prex_ratio+RaceConfig.TeamCode+"_"+timestamp, pcAmount>0 ? newValue/pcAmount : 0);
			System.out.println("Ratio Result: "+ (pcAmount>0 ? newValue/pcAmount : 0));
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
