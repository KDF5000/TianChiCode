package com.alibaba.middleware.race.Tair;

import java.io.Serializable;

public class TairData {
	public Serializable key;
	public Serializable value;
	
	public TairData(Serializable key, Serializable value){
		this.key = key;
		this.value = value;
	}
}
