package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {
	
	protected DefaultTairManager tairManager = null;
	protected List<String> confServer = null;
	protected int namespace = 0; //默认是0
//	protected int DEFAULT_EXPIRE_TIME = 1800; //秒，比赛的最长运行时间要求是20分钟，这里设置缓存30分钟
	
    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
		this.namespace = namespace;
		//创建config server
		this.confServer = new ArrayList<String>();
		this.confServer.add(masterConfigServer);
		
		//创建客户端
		this.tairManager = new DefaultTairManager();
		this.tairManager.setConfigServerList(this.confServer);
		
		//设置组名
		this.tairManager.setGroupName(groupName);
		
		this.tairManager.init();
    }

    public boolean write(Serializable key, Serializable value) {
    	ResultCode resultCode = this.tairManager.put(this.namespace, key, value);
//    	System.err.println(resultCode.getMessage());
    	if(resultCode.isSuccess()){
    		return true;
    	}
        return false;
    }

    public Object get(Serializable key) {
    	Result<DataEntry> result = tairManager.get(this.namespace, key);
	    DataEntry entry = null;
	    
	    if (result.isSuccess()) {
	      entry = result.getValue();
//	      System.out.println("res:"+entry);
	    } else {
	      // 异常处理
	      System.out.println(result.getRc().getMessage());
	    }
	    if(entry!=null)
	    	return entry.getValue();
	    return null;
    }

    public boolean remove(Serializable key) {
    	ResultCode resultCode = this.tairManager.delete(this.namespace, key);
    	if(resultCode.isSuccess()){
    		return true;
    	}
        return false;
    }

    public void close(){
    	if(this.tairManager != null){
    		this.tairManager.close();
    	}
    }
     
    public void init(){
    	this.tairManager.init();
    }

    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall +minuteTime, money);
        System.out.println(tairOperator.get(RaceConfig.prex_tmall + minuteTime));
        
        tairOperator.write(RaceConfig.prex_tmall+(minuteTime+1), money+100);
        System.out.println(tairOperator.get(RaceConfig.prex_tmall+(minuteTime+1)));
        tairOperator.close();
        
    }
}
