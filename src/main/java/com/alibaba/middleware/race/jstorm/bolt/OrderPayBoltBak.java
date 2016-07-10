package com.alibaba.middleware.race.jstorm.bolt;
/*package com.alibaba.middleware.race.jstorm.bolt;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.DataTuple;
import com.esotericsoftware.minlog.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


class OrderType{
	public int type;//0:TMALL_ORDER,1:TAOBAO_ORDER
	public double remainPrice;//剩余的订单金额
}

class PayMsg{
	public long orderId;
	public double amount;//金额
	public long timestamp;//争分的时间戳
}

public class OrderPayBolt implements IRichBolt {

	private static final long serialVersionUID = 138293829842L;
    private OutputCollector collector;
    private HashMap<Long, OrderType> orderTypeMap = null;
    private LinkedList<PayMsg> unemitPayMsg = null;
    
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.orderTypeMap = new HashMap<Long, OrderType>();
		this.unemitPayMsg = new LinkedList<PayMsg>();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		//"type", "orderId","amount","createdTime"
		int type = input.getIntegerByField("type");
		long orderId = input.getLongByField("orderId");
		double amount = input.getDoubleByField("amount");
		long timestamp = input.getLongByField("createdTime");
		
		if(type == DataTuple.MQ_TMALL_ORDER || type == DataTuple.MQ_TAOBAO_ORDER){
			//放到hashmap里
			OrderType orderType = new OrderType();
			orderType.type = type;
			orderType.remainPrice = amount;
			
			if(!this.orderTypeMap.containsKey(orderId)){
				//查询带消费队列看看是否存在该orderid的消息
				for(int i=0;i<this.unemitPayMsg.size();i++){
					PayMsg payMsg = this.unemitPayMsg.get(i);
					if(payMsg.orderId == orderId){
						orderType.remainPrice -= payMsg.amount;
						this.unemitPayMsg.remove(i);
						i--;//移除了一个元素所以要回退
//						System.err.println("<<<<<Emit:"+"[order_stat_"+orderType.type+","+payMsg.timestamp+","+ payMsg.amount+"]");
						this.collector.emit("order_stat_"+orderType.type, new Values(payMsg.timestamp, payMsg.amount));
					}
				}
				if(orderType.remainPrice > 0){
					this.orderTypeMap.put(orderId, orderType);
				}
			}else{
				//原则上order没有重复的
			}
		}else{//支付消息
			//查询map看看是否存在orderid
			if(this.orderTypeMap.containsKey(orderId)){
				OrderType orderType = this.orderTypeMap.get(orderId);
				orderType.remainPrice -= amount;
				if(orderType.remainPrice <=0){//该订单消耗完了，可以删除
					this.orderTypeMap.remove(orderId);
				}else{//重新放入map
					this.orderTypeMap.put(orderId, orderType);
				}
//				System.err.println("<<<<<Emit:"+"[order_stat_"+orderType.type+","+timestamp+","+ amount+"]");
				this.collector.emit("order_stat_"+orderType.type, new Values(timestamp, amount));
			}else{
				//暂时放到待消费队列里
				PayMsg payMsg = new PayMsg();
				payMsg.orderId = orderId;
				payMsg.amount = amount;
				payMsg.timestamp = timestamp;
				this.unemitPayMsg.add(payMsg);
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
		declarer.declareStream("order_stat_"+DataTuple.MQ_TAOBAO_ORDER, new Fields("timestamp", "amount"));
		declarer.declareStream("order_stat_"+DataTuple.MQ_TMALL_ORDER, new Fields("timestamp", "amount"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
*/