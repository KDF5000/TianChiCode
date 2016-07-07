package com.alibaba.middleware.race.jstorm.bolt;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.DataTuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterMessageBolt implements IRichBolt {

	private static final long serialVersionUID = 138293829842L;
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
		DataTuple dataTuple = (DataTuple) input.getValue(1);
		if(dataTuple.getType() == DataTuple.MQ_TMALL_ORDER || dataTuple.getType() == DataTuple.MQ_TAOBAO_ORDER){
			//放到tair里 key为订单id，然后放到tair里
			System.out.println("订单放到Tair->OrderId: "+ dataTuple.getOrderMessage().getOrderId()+" Type: "+dataTuple.getType());
			this.tairOperator.write(dataTuple.getOrderMessage().getOrderId(), dataTuple.getType());
		}else{
			//支付消息，传到下一个bolt处理 统计
			System.out.println("emit 支付消息: "+dataTuple.getPayMessage().getOrderId());
			this.collector.emit(new Values(dataTuple.getPayMessage(), dataTuple.getPayMessage().getPayPlatform()));
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
		declarer.declare(new Fields("filter_msg", "platform"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
