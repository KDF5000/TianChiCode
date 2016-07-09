package com.alibaba.middleware.race.Tair;

import java.util.concurrent.LinkedBlockingDeque;
import com.alibaba.middleware.race.RaceConfig;

public class TairRunnable implements Runnable {
	
	private static final TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
          RaceConfig.TairGroup, RaceConfig.TairNamespace);
	
	private LinkedBlockingDeque<TairData> tairDataList = null;
	
	public TairRunnable(LinkedBlockingDeque<TairData> list) {
		// TODO Auto-generated constructor stub
		this.tairDataList = list;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			TairData data = null;
			try {
				data = this.tairDataList.take();
			} catch (Exception e) {
				// TODO: handle exception
			}
			if(data == null){
				continue;
			}
			TairRunnable.tairOperator.write(data.key, data.value);
		}
		
	}

}
