package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";

    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    
    /****本地****/
    /*public static String TeamCode = "429038utrh";
    public static String JstormTopologyName = "429038utrh";
    public static String MetaConsumerGroup = "429038utrh";
    public static String TairConfigServer = "172.16.2.128:5198";
    public static String TairSalveConfigServer = "172.16.2.128:5198";
    public static String TairGroup = "group_1";
    public static Integer TairNamespace = 0;*/
    //rocketmq server
    public static String MqNameServer = "172.16.2.129:9876";
    /****本地****/
    
    /********Race ********/
    public static String TeamCode = "429038utrh";
    public static String JstormTopologyName = "429038utrh";
    public static String MetaConsumerGroup = "429038utrh";
    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 40652;
    /*******Race ********/
    
    /********Wang ********/
    /*public static String TeamCode = "46440tsy6h";
    public static String JstormTopologyName = "46440tsy6h";
    public static String MetaConsumerGroup = "46440tsy6h";
    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 41511;*/
    /********Wang ********/
    
    /********Yang De ********/
   /* public static String TeamCode = "429038utrh";
    public static String JstormTopologyName = "429038utrh";
    public static String MetaConsumerGroup = "429038utrh";
    public static String TairConfigServer = "192.168.122.212:5198";
    public static String TairSalveConfigServer = "192.168.122.212:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 1;
    public static String MqNameServer = "192.168.122.66:9876";*/
    /********Yang De ********/
   
    public static String MqGroup = "Hydra";
}
