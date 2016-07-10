package com.alibaba.middleware.race.jstorm.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.esotericsoftware.minlog.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

class RatioNode{
	long timestamp;//时间戳
	double amount;//该时间段内的交易额
	double totalAmount;//截止该时间段的交易额
}

public class PlatformRatioStatBolt implements IRichBolt{
	private static Logger LOG = LoggerFactory.getLogger(PlatformRatioStatBolt.class);
	private OutputCollector collector;
	private HashMap<Long, Integer> pcNodeMap = null;//时间戳和节点的索引
	private HashMap<Long, Integer> mobileNodeMap = null;//移动端map
	private LinkedList<RatioNode> pcAmountStat = null;
	private LinkedList<RatioNode> mobileAmountStat = null;
	
//	private LinkedBlockingDeque<TairData> tairDataList = null;
	private ConcurrentHashMap<Long, Double> dataCache = null;//暂时缓存要写入的数据
	
	private int count = 0;
	private TairOperatorImpl tairOperator = null;
	
	//DEBUG
//	private FileOutputStream out = null;
	//DEBUG
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.pcNodeMap = new HashMap<Long, Integer>();
		this.pcAmountStat = new LinkedList<RatioNode>();
		this.mobileNodeMap = new HashMap<Long, Integer>();
		this.mobileAmountStat = new LinkedList<RatioNode>();
		
		/*this.tairDataList = new LinkedBlockingDeque<TairData>();
		//启动一个线程专门去写tair
		Runnable tairRunnable = new TairRunnable(this.tairDataList);
		Thread thread = new Thread(tairRunnable);
		thread.start();	*/	
		
		this.dataCache = new ConcurrentHashMap<Long, Double>();
		this.count = 0;
		this.tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				System.err.println("启动线程!");
				while(true){
					try {
						Thread.sleep(12*1000);// 10s写一次
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					for (Entry<Long, Double> entry : dataCache.entrySet()) {
			            long key = entry.getKey();
			            double val = entry.getValue();
//			            System.err.println("Write: "+RaceConfig.prex_ratio+RaceConfig.TeamCode+"_"+key+","+ val);
			            tairOperator.write(RaceConfig.prex_ratio+RaceConfig.TeamCode+"_"+key, val);
			            //删除
			            dataCache.remove(key);
			        }
				}
			}
		}).start();
	}
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		short platform = input.getShortByField("platform");
		long timestamp = input.getLongByField("timestamp");
		double amount = input.getDoubleByField("amount");
		if(platform == 0){//PC
			this.updateStat(this.pcAmountStat, this.pcNodeMap, timestamp, amount);
		}else{//Mobile
			this.updateStat(this.mobileAmountStat, this.mobileNodeMap, timestamp, amount);
		}
		this.restatRatio(timestamp);
		
		this.collector.ack(input);
	}
	
	private void write2Tair(){
		Iterator<Entry<Long, Double>> entries = this.dataCache.entrySet().iterator();
		while(entries.hasNext()){
			Map.Entry entry = (Map.Entry) entries.next();  
			Long timestamp = (Long) entry.getKey();
			Double amount = (Double)entry.getValue();
			this.tairOperator.write(RaceConfig.prex_ratio+RaceConfig.TeamCode+"_"+timestamp.longValue(), RaceUtils.round(amount.doubleValue(), 2));
			entries.remove();
		}
	}
	
	/**
	 * 重新计算ratio
	 * @param modifiedTimestamp
	 */
	public void restatRatio( long modifiedTimestamp){
		//从modifiedtimestamp后的时间都要重新计算一下
		int fromIndex = 0;
		if(!this.pcNodeMap.containsKey(modifiedTimestamp)){
			if(!this.mobileNodeMap.containsKey(modifiedTimestamp)){
				return;
			}
			int mobileIndex = this.mobileNodeMap.get(modifiedTimestamp);
			int mobileSize = this.mobileAmountStat.size();
			for(int j=mobileIndex;j<mobileSize;j++){
				RatioNode node = this.mobileAmountStat.get(j);
				if(this.pcNodeMap.containsKey(node.timestamp)){
					fromIndex =this.pcNodeMap.get(node.timestamp);
					break;
				}
			}
		}else{
			fromIndex = this.pcNodeMap.get(modifiedTimestamp);
		}
		int size = this.pcAmountStat.size();
		for(int i=fromIndex;i<size;i++){
			RatioNode pcNode = this.pcAmountStat.get(i);
			if(this.mobileNodeMap.containsKey(pcNode.timestamp)){
				int index = this.mobileNodeMap.get(pcNode.timestamp);
				RatioNode mobileNode = this.mobileAmountStat.get(index);
				if(pcNode.totalAmount != 0){
					//计算ratio,写入tair
//					System.out.println("["+pcNode.timestamp+","+RaceUtils.round(mobileNode.totalAmount/pcNode.totalAmount, 2)+"]");
//					this.tairOperator.write(RaceConfig.prex_ratio+RaceConfig.TeamCode+"_"+pcNode.timestamp, RaceUtils.round(mobileNode.totalAmount/pcNode.totalAmount, 2));
//					this.tairDataList.offer(new TairData(RaceConfig.prex_ratio+RaceConfig.TeamCode+"_"+pcNode.timestamp, RaceUtils.round(mobileNode.totalAmount/pcNode.totalAmount, 2)));
					this.dataCache.put(pcNode.timestamp, RaceUtils.round(mobileNode.totalAmount/pcNode.totalAmount, 2));
					/*this.count++;
					if(this.count >= 2000){
						write2Tair();
						this.count = 0;
					}*/
					Log.info("["+RaceConfig.prex_ratio+RaceConfig.TeamCode+"_"+pcNode.timestamp+","+RaceUtils.round(mobileNode.totalAmount/pcNode.totalAmount, 2)+"]");
				}
			}
		}
	}
	/**
	 * 更新统计状态
	 * @param list
	 * @param nodeMap
	 * @param timestamp
	 * @param amount
	 */
	public void updateStat(LinkedList<RatioNode> list, HashMap<Long, Integer> nodeMap, long timestamp, double amount){
		if(nodeMap.containsKey(timestamp)){
			int index = nodeMap.get(timestamp);
			RatioNode hintNode = list.get(index);
			double diffAmount = amount - hintNode.amount;
			hintNode.amount = amount;//更新amount
			list.set(index, hintNode);
			//更新时间大于等于该时间戳的节点的totalamount
			this.updateTotalAmount(list, nodeMap, index, diffAmount);
		}else{
			//插入节点
			//创建一个新的节点
			RatioNode newNode = new RatioNode();
			newNode.amount = amount;
			newNode.timestamp = timestamp;
			newNode.totalAmount = 0;
			
			int insertIndex = this.insertList(list, newNode);
			
			if(insertIndex > 0){
				RatioNode node = list.get(insertIndex-1);//获取前一个节点的total
				newNode.totalAmount = node.totalAmount;
				list.set(insertIndex, newNode);//更新刚插入的节点
			}
			
			//更新从insertIndex之后所有节点的totalAmount
			this.updateTotalAmount(list, nodeMap,insertIndex, amount);
		}
	}
	/**
	 * 更新从fromindex之后所有节点的totalamount
	 * @param list
	 * @param formIndex
	 * @param diff
	 */
	public void updateTotalAmount(LinkedList<RatioNode> list, HashMap<Long, Integer>nodeMap, int formIndex, double diff){
		for(int i=formIndex;i<list.size();i++){
			RatioNode node = list.get(i);
			node.totalAmount += diff;
			list.set(i, node);
			//更新map的索引
			nodeMap.put(node.timestamp, i);
		}
	}
	
	/**
	 * 从尾遍历插入
	 * @param list
	 * @param newData
	 * @return
	 */
	public int insertList(LinkedList<RatioNode> list, RatioNode newData){
		int size = list.size();
		int i = size -1;
		for(;i>=0;i--){
			RatioNode node = list.get(i);
			if(newData.timestamp > node.timestamp){
				if(i == size-1){
					list.addLast(newData);
				}else{
					list.add(i+1, newData);
				}
				break;
			}
		}
		if(i==-1){
			list.addFirst(newData);
		}
		return i+1;
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
