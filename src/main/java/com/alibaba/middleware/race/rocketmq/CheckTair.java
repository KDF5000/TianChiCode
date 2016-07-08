package com.alibaba.middleware.race.rocketmq;

import com.alibaba.jstorm.common.metric.TopologyHistory;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

public class CheckTair {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		String tb_key = RaceConfig.prex_taobao+RaceConfig.TeamCode+"_"+1467915600;
		String tm_key = RaceConfig.prex_tmall+RaceConfig.TeamCode+"_"+1467915960;
		//platformTaobao_429038utrh_1467915960
//		platformTmall_429038utrh_1467914400
//		
//		tairOperator.write("platformTaobao", 1213131);
//		tairOperator.write("platformTmall", 1213131);
//		
		System.out.println(tairOperator.get("platformTaobao_429038utrh_1467961680"));
		System.out.println(tairOperator.get("platformTmall_429038utrh_1467961680"));
		
	}

}
