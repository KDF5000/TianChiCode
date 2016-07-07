package com.alibaba.middleware.race.jstorm.bolt;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

class AmountTotalCache{
	private long timestamp; //时间戳
	private double amount; //总金额
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public double getAmount() {
		return amount;
	}
	public void setAmount(double amount) {
		this.amount = amount;
	}
}

public class RatioStatBolt implements IRichBolt{
	private OutputCollector collector;
	private TairOperatorImpl tairOperator = null;
	private TopologyContext context;
	private AmountTotalCache lastPcAmountTotal = null;
	private AmountTotalCache lastMobileAmountTotal = null;
	private long mobileCurrentTime = 0;
	private LinkedList<AmountTotalCache>  pcAmountTotalCache = null;
	private LinkedList<AmountTotalCache>  mobileAmountTotalCache = null;
	private int cacheSize = 10;
	private AmountTotalCache[] lastAmountTotalCaches = new AmountTotalCache[2];
	private ArrayList<LinkedList<AmountTotalCache>> totalAmounts = new ArrayList<LinkedList<AmountTotalCache>>();
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		this.context = context;
		this.totalAmounts.add(new LinkedList<AmountTotalCache>());
		this.totalAmounts.add(new LinkedList<AmountTotalCache>());
	}
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		short platform = input.getShort(0);
		long createdTime = input.getLong(1);
		double amount = input.getDouble(2);
		System.out.println("Context:"+this.context.getThisTaskId()+",Platform:"+platform+",createdTime:"+createdTime+",amount:"+amount);
        AmountTotalCache lastAmountTotal;
        LinkedList<AmountTotalCache> amountTotalCaches;
		if(platform == 0){//PC
			if(this.lastPcAmountTotal==null){
				//说明是第一个tuple
				AmountTotalCache amountTotalCache = new AmountTotalCache();
				amountTotalCache.setTimestamp(createdTime);
				amountTotalCache.setAmount(amount);
				this.lastPcAmountTotal = amountTotalCache;
				this.pcAmountTotalCache.add(amountTotalCache);
			}else{
				if(this.lastPcAmountTotal.getTimestamp() == createdTime){
					//更新
					this.lastPcAmountTotal.setAmount(this.lastMobileAmountTotal.getAmount()+amount);
					this.pcAmountTotalCache.set(this.pcAmountTotalCache.size()-1, this.lastPcAmountTotal);
				}else if(lastPcAmountTotal.getTimestamp() < createdTime){
					if(this.pcAmountTotalCache.size() >= cacheSize){
						this.pcAmountTotalCache.removeFirst();
					}
					AmountTotalCache amountTotalCache = new AmountTotalCache();
					amountTotalCache.setTimestamp(createdTime);
					amountTotalCache.setAmount(amount);
					this.pcAmountTotalCache.add(amountTotalCache);
				}else{
					//遍历更新比createdtime大的或者插入
					for(int i=this.pcAmountTotalCache.size()-1;i>=0;i--){
						AmountTotalCache amountTotalCache = this.pcAmountTotalCache.get(i);
						if(amountTotalCache.getTimestamp() > createdTime){
							amountTotalCache.setAmount(amountTotalCache.getAmount()+amount);
							this.pcAmountTotalCache.set(i, amountTotalCache);
						}else if(amountTotalCache.getTimestamp()==createdTime){
							amountTotalCache.setAmount(amountTotalCache.getAmount()+amount);
							this.pcAmountTotalCache.set(i, amountTotalCache);
							break;
						}else{
							if(this.pcAmountTotalCache.size() >= cacheSize){
								this.pcAmountTotalCache.removeFirst();
							}
							AmountTotalCache newAmountTotalCache = new AmountTotalCache();
							newAmountTotalCache.setTimestamp(createdTime);
							newAmountTotalCache.setAmount(amount);
							this.pcAmountTotalCache.add(i+1, newAmountTotalCache);
							break;
						}
					}
				}
			}
		}
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
