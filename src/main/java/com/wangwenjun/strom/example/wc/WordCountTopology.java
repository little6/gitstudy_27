package com.wangwenjun.strom.example.wc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

public class WordCountTopology
{
    public static void main(String[] args) throws InterruptedException
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("LineSpout", new LineSpout(), 1);
        builder.setBolt("WordsSplitBolt", new WordsSplitBolt(), 4).localOrShuffleGrouping("LineSpout");
        builder.setBolt("WordsCountBolt", new WordsCountBolt(), 2).globalGrouping("WordsSplitBolt");

        final Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(false);

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCountTopology", conf, builder.createTopology());

        TimeUnit.SECONDS.sleep(60);
        cluster.killTopology("WordCountTopology");
        cluster.shutdown();


    }
}
