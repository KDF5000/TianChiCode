package com.alibaba.middleware.race.jstorm.bolt;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.Tair.TairRunnable;
import com.esotericsoftware.minlog.Log;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TbOrderStatBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(TbOrderStatBolt.class);
	private OutputCollector collector;
	private TairOperatorImpl tairOperator = null;
	private HashMap<Long, Double> orderResult = null;
	//debug
//	private FileOutputStream out = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.orderResult = new HashMap<Long, Double>();
//		this.tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
//                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		/*try {
			out = new FileOutputStream("tb.out");
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
		
		Runnable tairRunnable = new TairRunnable(RaceConfig.prex_taobao+RaceConfig.TeamCode+"_"+timestamp, RaceUtils.round(newAmount, 2));
		Thread thread = new Thread(tairRunnable);
		thread.start();

//		boolean res = this.tairOperator.write(RaceConfig.prex_taobao+RaceConfig.TeamCode+"_"+timestamp, RaceUtils.round(newAmount, 2));
		Log.info(">>>>>>["+RaceConfig.prex_taobao+RaceConfig.TeamCode+"_"+timestamp+","+newAmount+"]");
		
		/*try {
			out.write(("["+RaceConfig.prex_taobao+RaceConfig.TeamCode+"_"+timestamp+","+newAmount+"]\n").getBytes());
			out.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
//		this.collector.ack(input);
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
