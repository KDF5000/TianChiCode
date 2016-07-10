package com.alibaba.middleware.race.jstorm.bolt;

import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class PayMinuteStatBolt implements IRichBolt {

	private static final long serialVersionUID = 138293829842L;
    private OutputCollector collector;
    private HashMap<String,Double> minuteTotalMap;
    
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.minuteTotalMap = new HashMap<String, Double>();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		//"type", "orderId","amount","createdTime"
		short platform = input.getShortByField("platform");
		double amount = input.getDoubleByField("amount");
		long timestamp = input.getLongByField("createdTime");
		String key = platform+"_"+timestamp;
		double totalAmount = amount;
		if(this.minuteTotalMap.containsKey(key)){
			totalAmount += this.minuteTotalMap.get(key);
		}
		this.minuteTotalMap.put(key, totalAmount);//放入map
		this.collector.emit(new Values(platform, timestamp,totalAmount));
		
		this.collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("platform","timestamp", "amount"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
