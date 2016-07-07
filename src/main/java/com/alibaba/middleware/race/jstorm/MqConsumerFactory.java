package com.alibaba.middleware.race.jstorm;

import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

public class MqConsumerFactory {
	
	private static final Logger	LOG   = Logger.getLogger(MqConsumerFactory.class);
	
    public static Map<String, DefaultMQPushConsumer> consumers = 
    		new HashMap<String, DefaultMQPushConsumer>();
    
    public static synchronized DefaultMQPushConsumer mkInstance(MessageListenerConcurrently listener)  throws Exception{
    	
    	String key =  "consumer@" + RaceConfig.MqGroup;
    	
    	DefaultMQPushConsumer consumer = consumers.get(key);
    	if (consumer != null) {
    		LOG.info("Consumer of " + key + " has been created, don't need to recreate it ");
    		return consumer;
    	}
        
        consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
        
//        consumer.setNamesrvAddr(RaceConfig.MqNameServer);
        String instanceName = RaceConfig.MqGroup +"@" +	JStormUtils.process_pid();
		consumer.setInstanceName(instanceName);
//		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		
		consumer.subscribe(RaceConfig.MqPayTopic, "*");
		consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
		consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
		
		consumer.registerMessageListener(listener);

		consumer.start();
		
		consumers.put(key, consumer);
		LOG.info("Successfully create " + key + " consumer");
		
		return consumer;
    }
    
}

