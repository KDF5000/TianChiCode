package com.alibaba.middleware.race;

import java.math.BigDecimal;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class RaceUtils {
    /**
     * 由于我们是将消息进行Kryo序列化后，堆积到RocketMq，所有选手需要从metaQ获取消息，
     * 反序列出消息模型，只要消息模型的定义类似于OrderMessage和PaymentMessage即可
     * @param object
     * @return
     */
    public static byte[] writeKryoObject(Object object) {
        Output output = new Output(1024);
        Kryo kryo = new Kryo();
        kryo.writeObject(output, object);
        output.flush();
        output.close();
        byte [] ret = output.toBytes();
        output.clear();
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
        Kryo kryo = new Kryo();
        Input input = new Input(bytes);
        input.close();
        T ret = kryo.readObject(input, tClass);
        return ret;
    }
    
    public static Double round(double value, int decemial){
    	BigDecimal bd = new BigDecimal(value);
    	BigDecimal  bd2 = bd.setScale(decemial,BigDecimal.ROUND_HALF_UP);
    	return Double.parseDouble(bd2.toString());
    }
    
    public static long getMinuteTime(long timestamp){
    	return (timestamp / 1000 / 60) * 60;
    }

}
