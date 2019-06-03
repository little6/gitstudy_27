package com.wangwenjun.strom.example.grouping.custom;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

public class LocalLogTopology
{
    public static void main(String[] args) throws InterruptedException
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("LogSpout", new LogSpout(), 1);
        builder.setBolt("LogParseBolt", new LogParseBolt(), 1).localOrShuffleGrouping("LogSpout");
//        builder.setBolt("LogPrintBolt", new LogPrintBolt(), 2).customGrouping("LogParseBolt", new HighestTaskIdGrouping());
        builder.setBolt("LogPrintBolt", new LogPrintBolt(), 2).customGrouping("LogParseBolt", new CategoryGrouping());

        final Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(4);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalLogTopology", config, builder.createTopology());
        TimeUnit.SECONDS.sleep(120);

        cluster.killTopology("LocalLogTopology");
        cluster.shutdown();
    }
}
