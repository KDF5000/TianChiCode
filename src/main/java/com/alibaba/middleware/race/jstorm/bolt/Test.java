/*package com.alibaba.middleware.race.jstorm.bolt;

import java.util.HashMap;
import java.util.LinkedList;

import org.omg.CORBA.INTERNAL;

import com.alibaba.middleware.race.RaceUtils;


class RatioNode{
	long timestamp;//时间戳
	double amount;//该时间段内的交易额
	double totalAmount;//截止该时间段的交易额
}

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Test test = new Test();
		test.execute(0, 1467915601, 12.0);
		
		test.execute(1, 1467915601, 12.0);
		test.execute(1, 1467915605, 12.0);
		
		
		test.execute(0, 1467915605, 12.0);
		
		test.execute(0, 1467915603, 10);//中间
		test.execute(0, 1467915603, 15);//更新存在节点
		test.execute(0, 1467915600, 10);//头
		test.execute(0, 1467915606, 10);//尾部
		
		
		
		test.execute(1, 1467915603, 10);//中间
		test.execute(1, 1467915603, 15);//更新存在节点
		test.execute(1, 1467915500, 10);//头
		test.execute(1, 1467915606, 10);//尾部
		
		test.printInfo();

	}
	
	private HashMap<Long, Integer> pcNodeMap = null;//时间戳和节点的索引
	private HashMap<Long, Integer> mobileNodeMap = null;//移动端map
	private LinkedList<RatioNode> pcAmountStat = null;
	private LinkedList<RatioNode> mobileAmountStat = null;
	
	public Test(){
		this.init();
	}
	public void init() {
		// TODO Auto-generated method stub
		this.pcNodeMap = new HashMap<Long, Integer>();
		this.pcAmountStat = new LinkedList<RatioNode>();
		this.mobileNodeMap = new HashMap<Long, Integer>();
		this.mobileAmountStat = new LinkedList<RatioNode>();
	}
	
	public void printInfo(){
		System.out.println("----PC-----");
		for(int i=0;i<this.pcAmountStat.size();i++){
			RatioNode node = this.pcAmountStat.get(i);
			System.out.println("["+node.timestamp+","+node.amount+","+node.totalAmount+"]"+"{"+i+"<->"+this.pcNodeMap.get(node.timestamp)+"}");
			
		}
		System.out.println("----Mobile-----");
		for(int i=0;i<this.mobileAmountStat.size();i++){
			RatioNode node = this.mobileAmountStat.get(i);
			System.out.println("["+node.timestamp+","+node.amount+","+node.totalAmount+"]"+"{"+i+"<->"+this.mobileNodeMap.get(node.timestamp)+"}");
		}
	}
	public void execute(int platform, long timestamp, double amount) {
		// TODO Auto-generated method stub
		if(platform == 0){//PC
			this.updateStat(this.pcAmountStat, this.pcNodeMap, timestamp, amount);
		}else{//Mobile
			this.updateStat(this.mobileAmountStat, this.mobileNodeMap, timestamp, amount);
		}
		this.restatRatio(timestamp);
	}
	*//**
	 * 重新计算ratio
	 * @param modifiedTimestamp
	 *//*
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
				if(mobileNode.totalAmount != 0){
					//计算ratio,写入tair
					System.out.println("["+pcNode.timestamp+","+RaceUtils.round(pcNode.totalAmount/mobileNode.totalAmount, 2)+"]");
				}
			}
		}
	}
	*//**
	 * 更新统计状态
	 * @param list
	 * @param nodeMap
	 * @param timestamp
	 * @param amount
	 *//*
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
			nodeMap.put(timestamp, insertIndex);
		}
	}
	*//**
	 * 更新从fromindex之后所有节点的totalamount
	 * @param list
	 * @param formIndex
	 * @param diff
	 *//*
	public void updateTotalAmount(LinkedList<RatioNode> list, HashMap<Long, Integer>nodeMap, int formIndex, double diff){
		for(int i=formIndex;i<list.size();i++){
			RatioNode node = list.get(i);
			node.totalAmount += diff;
			list.set(i, node);
			//更新map的索引
			nodeMap.put(node.timestamp, i);
		}
	}
	
	*//**
	 * 从尾遍历插入
	 * @param list
	 * @param newData
	 * @return
	 *//*
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
 
}
*/