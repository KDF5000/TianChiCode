package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;

import java.util.List;


/**
 * Consumer，订阅消息
 */

/**
 * RocketMq消费组信息我们都会再正式提交代码前告知选手
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("429038utrh");

        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //在本地搭建好broker后,记得指定nameServer的地址
        consumer.setNamesrvAddr("172.16.2.129:9876");

        consumer.subscribe(RaceConfig.MqPayTopic, "*");
        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
		consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
			
			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				// TODO Auto-generated method stub
				for (MessageExt msg : msgs) {

                    byte [] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        //Info: 生产者停止生成数据, 并不意味着马上结束
                        System.out.println("Got the end signal");
                        continue;
                    }
                    if(msg.getTopic().equals(RaceConfig.MqPayTopic)){
                    	PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                        System.out.println(paymentMessage);
                    }else{
                    	OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                    	System.out.println(orderMessage);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

        consumer.start();

        System.out.println("Consumer Started.");
    }
}
