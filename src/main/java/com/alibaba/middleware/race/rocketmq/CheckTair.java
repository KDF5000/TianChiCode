package com.alibaba.middleware.race.rocketmq;

import java.util.LinkedList;

import com.alibaba.jstorm.common.metric.TopologyHistory;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.lmax.disruptor.LiteBlockingWaitStrategy;

public class CheckTair {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
//                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		String tb_key = RaceConfig.prex_taobao+RaceConfig.TeamCode+"_"+1467915600;
		String tm_key = RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+1467915960;
		//platformTaobao_429038utrh_1467915960
//		platformTmall_429038utrh_1467914400
//		
//		tairOperator.write("platformTaobao", 1213131);
//		tairOperator.write("platformTmall", 1213131);
//		
//		System.out.println(tairOperator.get("platformTaobao_429038utrh_1467961680"));
//		System.out.println(tairOperator.get("platformTmall_429038utrh_1467961680"));
		/*
		LinkedList<Integer> list = new LinkedList<Integer>();
		for(int i=0;i<10;i++){
			list.add(i);
		}
		
		for(int i=0;i<list.size();i++){
			int data = list.get(i);
			System.out.println("element:"+list.get(i));
			if(data==3){
				list.remove(i);
				i--;
			}
		}
		*/
		LinkedList<Integer> linkedList = new LinkedList<Integer>();
		
		linkedList.add(2);
		linkedList.add(3);
		linkedList.add(4);
		linkedList.add(6);
		linkedList.add(7);
		linkedList.add(8);
		
//		System.out.println(CheckTair.insertList(linkedList, 9));
		System.out.println(CheckTair.insertListTail(linkedList, 9));
		
		for(int j=0;j<linkedList.size();j++){
			System.out.println(linkedList.get(j));
		}
		
	}
	/**
	 * 从头开始插入
	 * @param list
	 * @param newData
	 * @return
	 */
	public static int insertList(LinkedList<Integer> list, int newData){
		int i = 0;
		for(;i<list.size();i++){
			int node = list.get(i);
			if(newData < node){
				if(i==0){
					list.addFirst(newData);
				}else{
					list.add(i, newData);
				}
				break;
			}
		}
		if(i == list.size()){
			//说明到末尾了
			list.addLast(newData);
		}
		return i;
	}
	/**
	 * 从尾遍历插入
	 * @param list
	 * @param newData
	 * @return
	 */
	public static int insertListTail(LinkedList<Integer> list, int newData){
		int size = list.size();
		int i = size -1;
		for(;i>=0;i--){
			int node = list.get(i);
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
}
