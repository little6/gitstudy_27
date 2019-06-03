package com.wangwenjun.strom.example.tick;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

public class TickTopology
{
    public static void main(String[] args) throws InterruptedException
    {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("NumberSpout", new NumberSpout(), 1);
        builder.setBolt("NumberBolt", new NumberBolt(), 2).globalGrouping("NumberSpout");

        final Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(false);

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TickTopology", conf, builder.createTopology());

        TimeUnit.SECONDS.sleep(60);
        cluster.killTopology("TickTopology");
        cluster.shutdown();


    }
}
