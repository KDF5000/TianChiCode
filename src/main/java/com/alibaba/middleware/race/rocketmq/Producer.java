
package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;

/**
 * Producer，发送消息
 */
public class Producer {

    private static Random rand = new Random();
    private static int count = 30000;
    private static HashMap<String, Double> orderResult = new HashMap<String, Double>();
    private static HashMap<Long, Double> pcTotal = new HashMap<Long, Double>();
    private static HashMap<Long, Double> mobileTotal = new HashMap<Long, Double>();
    
    /**
	 * 从尾遍历插入
	 * @param list
	 * @param newData
	 * @return
	 */
	public static int insertListTail(LinkedList<Long> list, long newData){
		int size = list.size();
		int i = size -1;
		for(;i>=0;i--){
			long node = list.get(i);
			if(newData > node){
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
	static class Node{
		long timestamp;
		double total;
	}
    public static void printRatio(){
    	LinkedList<Long> pcList = new LinkedList<Long>();
    	for(Map.Entry<Long, Double> entry: Producer.pcTotal.entrySet()){
    		Producer.insertListTail(pcList, entry.getKey());
    	}
    	
    	LinkedList<Long> mobileList = new LinkedList<Long>();
    	for(Map.Entry<Long, Double> entry: Producer.mobileTotal.entrySet()){
    		Producer.insertListTail(mobileList, entry.getKey());
    	}
    	/***顺序打印每分钟的总量**/
    	System.out.println("---------PC----------");
    	for(int i=0;i<pcList.size();i++){
    		long timestamp = pcList.get(i);
    		System.out.print(timestamp+":"+Producer.pcTotal.get(timestamp)+",");
    	}
    	System.out.println("\n---------PC----------");
    	System.out.println("---------MOBILE----------");
    	for(int i=0;i<mobileList.size();i++){
    		long timestamp = mobileList.get(i);
    		System.out.print(timestamp+":"+Producer.mobileTotal.get(timestamp)+",");
    	}
    	System.out.println("\n---------MOBILE----------");
    	/***顺序打印每分钟的总量**/
    	
    	LinkedList<Producer.Node> pcTotalList = new LinkedList<Producer.Node>();
    	double lastTotal = 0;
    	for(int i=0;i<pcList.size();i++){
    		Producer.Node node = new Producer.Node();
    		long timestamp = pcList.get(i);
    		node.timestamp = timestamp;
    		node.total = lastTotal + Producer.pcTotal.get(timestamp);
    		pcTotalList.add(node);
    		lastTotal = node.total;
    	}
    	LinkedList<Producer.Node> mobileTotalList = new LinkedList<Producer.Node>();
    	lastTotal = 0;
    	for(int i=0;i<mobileList.size();i++){
    		Producer.Node node = new Producer.Node();
    		long timestamp = mobileList.get(i);
    		node.timestamp = timestamp;
    		node.total = lastTotal + Producer.mobileTotal.get(timestamp);
    		mobileTotalList.add(node);
    		lastTotal = node.total;
    	}
    	/***打印叠加总量****/
    	System.out.println("---------PC TOTAL----------");
    	for(int i=0;i<pcTotalList.size();i++){
    		Producer.Node node = pcTotalList.get(i);
    		System.out.print(node.timestamp+":"+node.total+",");
    	}
    	System.out.println("\n---------PC TOTAL----------");
    	System.out.println("---------MOBILE TOTAL----------");
    	for(int i=0;i<mobileTotalList.size();i++){
    		Producer.Node node = mobileTotalList.get(i);
    		System.out.print(node.timestamp+":"+node.total+",");
    	}
    	System.out.println("\n---------MOBILE TOTAL----------");
    	/***打印叠加总量****/
    	
    	int i=0,j =0;
    	while(i<pcTotalList.size() && j < mobileTotalList.size()){
    		Producer.Node pcNode = pcTotalList.get(i);
    		Producer.Node mobileNode = mobileTotalList.get(j);
    		if(pcNode.timestamp == mobileNode.timestamp && pcNode.total!=0){
    			System.out.println("["+RaceConfig.prex_ratio+RaceConfig.TeamCode+"_"+pcNode.timestamp+","+RaceUtils.round(mobileNode.total/pcNode.total, 2)+"]");
    			i++;j++;
    		}else{
    			if(pcNode.timestamp < mobileNode.timestamp){
    				i++;
    			}else{
    				j++;
    			}
    		}
    	}
    }
    
    public static void addTotal(PaymentMessage paymentMessage){
    	long timestamp = RaceUtils.getMinuteTime(paymentMessage.getCreateTime());
    	if(paymentMessage.getPayPlatform() == 0){
    		double newAmount = paymentMessage.getPayAmount();
    		if(Producer.pcTotal.containsKey(timestamp)){
    			newAmount += Producer.pcTotal.get(timestamp);
    		}
    		Producer.pcTotal.put(timestamp, newAmount);
    	}else{
    		double newAmount = paymentMessage.getPayAmount();
    		if(Producer.mobileTotal.containsKey(timestamp)){
    			newAmount += Producer.mobileTotal.get(timestamp);
    		}
    		Producer.mobileTotal.put(timestamp, newAmount);
    	}
    }
    public static void addResult(OrderMessage orderMessage, PaymentMessage paymentMessage, int platform){
    	long timestamp = RaceUtils.getMinuteTime(paymentMessage.getCreateTime());
    	String key = platform == 0 ? RaceConfig.prex_taobao+RaceConfig.TeamCode+"_"+timestamp : RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+timestamp;
    	double amount = paymentMessage.getPayAmount();
    	if(Producer.orderResult.containsKey(key)){
    		amount += Producer.orderResult.get(key);
    	}
    	Producer.orderResult.put(key, amount);
    }
    
    public static void printOrderResult(){
    	for(Map.Entry<String, Double> entry: Producer.orderResult.entrySet()){
    		System.out.println("["+entry.getKey()+","+RaceUtils.round(entry.getValue(), 2)+"]");
    	}
    }

    /**
     * 这是一个模拟堆积消息的程序，生成的消息模型和我们比赛的消息模型是一样的，
     * 所以选手可以利用这个程序生成数据，做线下的测试。
     * @param args
     * @throws MQClientException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws MQClientException, InterruptedException {
    	//指定生成条数
    	if(args != null && args.length > 0){
    		count = Integer.valueOf(args[0]);
    	}
    	
        DefaultMQProducer producer = new DefaultMQProducer("429038utrh");

        //在本地搭建好broker后,记得指定nameServer的地址
        producer.setNamesrvAddr(RaceConfig.MqNameServer);

        producer.start();

        final String [] topics = new String[]{RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic};
        final Semaphore semaphore = new Semaphore(0);

        for (int i = 0; i < count; i++) {
            try {
            	Thread.sleep(10);
                final int platform = rand.nextInt(2);
                final OrderMessage orderMessage = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
                orderMessage.setCreateTime(System.currentTimeMillis());

                byte [] body = RaceUtils.writeKryoObject(orderMessage);

                Message msgToBroker = new Message(topics[platform], body);

                producer.send(msgToBroker, new SendCallback() {
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(orderMessage);
                        semaphore.release();
                    }
                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });

                //Send Pay message
                PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
                double amount = 0;
                for (final PaymentMessage paymentMessage : paymentMessages) {
                	////////////
                	Producer.addResult(orderMessage, paymentMessage, platform);
                	Producer.addTotal(paymentMessage);
                	////////////
                    int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
                    if (retVal < 0) {
                        throw new RuntimeException("price < 0 !!!!!!!!");
                    }

                    if (retVal > 0) {
                        amount += paymentMessage.getPayAmount();
                        final Message messageToBroker = new Message(RaceConfig.MqPayTopic, RaceUtils.writeKryoObject(paymentMessage));
                        producer.send(messageToBroker, new SendCallback() {
                            public void onSuccess(SendResult sendResult) {
                                System.out.println(paymentMessage);
                            }
                            public void onException(Throwable throwable) {
                                throwable.printStackTrace();
                            }
                        });
                    }else {
                        //
                    }
                }

                if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
                    throw new RuntimeException("totalprice is not equal.");
                }


            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        semaphore.acquire(count);

        //用一个short标识生产者停止生产数据
        byte [] zero = new  byte[]{0,0};
        Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
        Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
        Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);

        try {
            producer.send(endMsgTB);
            producer.send(endMsgTM);
            producer.send(endMsgPay);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.shutdown();
        System.out.println("开始计算数据...");
        Producer.printOrderResult();
//        Producer.printRatio();
    }
}
