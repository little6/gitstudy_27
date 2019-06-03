package com.wangwenjun.strom.example.helloworld;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

public class RandomStringTopologyLocal
{
    public static void main(String[] args) throws InterruptedException
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RandomStringSpout", new RandomStringSpout());
        builder.setBolt("WrapStarBolt", new WrapStarBolt()).shuffleGrouping("RandomStringSpout");
        builder.setBolt("WrapWellBolt", new WrapWellBolt()).shuffleGrouping("RandomStringSpout");

        final Config conf = new Config();
        conf.setDebug(true);

        conf.setNumWorkers(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RandomStringTopologyLocal", conf, builder.createTopology());

        System.out.println("The first topology is start running at local");

        TimeUnit.SECONDS.sleep(30);

        cluster.killTopology("RandomStringTopologyLocal");
        cluster.shutdown();
    }
}
