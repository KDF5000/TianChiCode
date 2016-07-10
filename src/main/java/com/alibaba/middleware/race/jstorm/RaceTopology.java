package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.bolt.OrderPayBolt;
import com.alibaba.middleware.race.jstorm.bolt.PayMinuteStatBolt;
import com.alibaba.middleware.race.jstorm.bolt.PlatformRatioStatBolt;
import com.alibaba.middleware.race.jstorm.bolt.TbOrderStatBolt;
import com.alibaba.middleware.race.jstorm.bolt.TmOrderStatBolt;
import com.alibaba.middleware.race.model.DataTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {
        Config config = new Config();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new MqSpout(), 2);
        
        builder.setBolt("pay_minute", new PayMinuteStatBolt(),2).fieldsGrouping("spout", "ratio_out", new Fields("createdTime"));
        builder.setBolt("ratio_stat", new PlatformRatioStatBolt(), 1).shuffleGrouping("pay_minute");
        
        builder.setBolt("order_pay", new OrderPayBolt(), 2).fieldsGrouping("spout","order_pay", new Fields("orderId"));
        builder.setBolt("tm_stat", new TmOrderStatBolt(), 2).fieldsGrouping("order_pay", "order_stat_"+DataTuple.MQ_TMALL_ORDER, new Fields("timestamp"));
        builder.setBolt("tb_stat", new TbOrderStatBolt(), 2).fieldsGrouping("order_pay", "order_stat_"+DataTuple.MQ_TAOBAO_ORDER, new Fields("timestamp"));
        
        String topologyName = RaceConfig.JstormTopologyName;

//        config.setNumAckers(0);
      //通过是否有参数来控制是否启动集群，或者本地模式执行
//        if(args != null && args.length > 0) {
            try {
                config.setNumWorkers(3);
                StormSubmitter.submitTopology(topologyName, config,
                        builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
//        } else {
//          config.setMaxTaskParallelism(1);
//          LocalCluster cluster = new LocalCluster();
//          cluster.submitTopology(topologyName, config, builder.createTopology());
//        }
    }
}