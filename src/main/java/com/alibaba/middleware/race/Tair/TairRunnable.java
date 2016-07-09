package com.alibaba.middleware.race.Tair;

import java.io.Serializable;

import com.alibaba.middleware.race.RaceConfig;


public class TairRunnable implements Runnable {
	private static final TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
          RaceConfig.TairGroup, RaceConfig.TairNamespace);
	private Serializable key;
	private Serializable value;
	
	public TairRunnable(Serializable key,Serializable value) {
		// TODO Auto-generated constructor stub
		this.key = key;
		this.value = value;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		TairRunnable.tairOperator.write(key, value);
	}

}
