package com.alibaba.middleware.race.jstorm.bolt;

import java.util.HashMap;
import java.util.Map;
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
	private LinkedBlockingDeque<TairData> tairDataList = null;
	//dbug
//	private FileOutputStream out = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.orderResult = new HashMap<Long, Double>();
		
		this.tairDataList = new LinkedBlockingDeque<TairData>();
		//启动一个线程专门去写tair
		Runnable tairRunnable = new TairRunnable(this.tairDataList);
		Thread thread = new Thread(tairRunnable);
		thread.start();		
		
		/*try {
			out = new FileOutputStream("tm.out");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
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
		
//		boolean res = this.tairOperator.write(RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+timestamp, RaceUtils.round(newAmount, 2));
		this.tairDataList.offer(new TairData(RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+timestamp, RaceUtils.round(newAmount, 2)));
		Log.info(">>>>>>["+RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+timestamp+","+newAmount+"]");
		
		/*try {
			out.write(("["+RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+timestamp+","+newAmount+"]\n").getBytes());
			out.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
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
