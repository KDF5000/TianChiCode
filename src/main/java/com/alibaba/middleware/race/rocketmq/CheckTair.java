package com.alibaba.middleware.race.rocketmq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

public class CheckTair {
	public static TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
            RaceConfig.TairGroup, RaceConfig.TairNamespace);
	
	//[platformTaobao_429038utrh_1468056360,1.26913586E7]
	public static void checkLine(String line){
		int len = line.length();
		String []data = line.substring(1, len-1).split(",");
		String key = data[0];
		double value = new BigDecimal(data[1]).doubleValue();
		Object tairValObj = CheckTair.tairOperator.get(key);
		if(tairValObj == null){
			System.err.println("False: "+line);
			return;
		}
		double tairVal = new BigDecimal(tairValObj.toString()).doubleValue();
		if(Math.abs(tairVal-value) <= 0.01){
			System.out.println("True: "+line+" "+tairVal);
		}else{
			System.err.println("False: "+line+" "+tairVal);
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		File file = new File("out.data");
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
            	CheckTair.checkLine(tempString);
            }
            reader.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
		CheckTair.tairOperator.close();
	}
}
