package com.alibaba.middleware.race.jstorm.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairData;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.Tair.TairRunnable;
import com.esotericsoftware.minlog.Log;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TmOrderStatBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(TmOrderStatBolt.class);
	private OutputCollector collector;
	private HashMap<Long, Double> orderResult = null;
//	private LinkedBlockingDeque<TairData> tairDataList = null;
	
	private ConcurrentHashMap<Long, Double>dataCache = new ConcurrentHashMap<Long, Double>();;//暂时缓存要写入的数据
//	private int count = 0;
	private TairOperatorImpl tairOperator = null;
	
	//dbug
//	private FileOutputStream out = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.orderResult = new HashMap<Long, Double>();
		
		/*this.tairDataList = new LinkedBlockingDeque<TairData>();
		//启动一个线程专门去写tair
		Runnable tairRunnable = new TairRunnable(this.tairDataList);
		Thread thread = new Thread(tairRunnable);
		thread.start();		*/
		
		this.tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		this.dataCache = new ConcurrentHashMap<Long, Double>();
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				System.err.println("启动线程!");
				while(true){
					try {
						Thread.sleep(10*1000);// 10s写一次
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					for (Entry<Long, Double> entry : dataCache.entrySet()) {
			            long key = entry.getKey();
			            double val = entry.getValue();
			            System.err.println("Write: "+RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+key+","+ RaceUtils.round(val, 2));
			            tairOperator.write(RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+key, RaceUtils.round(val, 2));
			            //删除
			            dataCache.remove(key);
			        }
				}
				
			}
		}).start();
//		this.count = 0;
	}
	
	private void write2Tair(){
		Iterator<Entry<Long, Double>> entries = this.dataCache.entrySet().iterator();
		while(entries.hasNext()){
			Map.Entry entry = (Map.Entry) entries.next();  
			Long timestamp = (Long) entry.getKey();
			Double amount = (Double)entry.getValue();
			this.tairOperator.write(RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+timestamp.longValue(), RaceUtils.round(amount.doubleValue(), 2));
			entries.remove();
		}
	}
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		long timestamp = input.getLongByField("timestamp");
		Double amount = input.getDoubleByField("amount");
		double newAmount = amount;
		if(this.orderResult.containsKey(timestamp)){
			newAmount += this.orderResult.get(timestamp);
		}
		this.orderResult.put(timestamp, newAmount);
		this.dataCache.put(timestamp, newAmount);
		/*this.count++;
		if(this.count >= 2000){
			write2Tair();
			this.count = 0;
		}*/
//		boolean res = this.tairOperator.write(RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+timestamp, RaceUtils.round(newAmount, 2));
//		this.tairDataList.offer(new TairData(RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+timestamp, RaceUtils.round(newAmount, 2)));
		Log.info(">>>>>>["+RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+timestamp+","+newAmount+"]");
		
		this.collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
//		write2Tair();
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
