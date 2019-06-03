package com.wangwenjun.strom.example.telephone;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

public class TelephoneCallLogTopology
{
    public static void main(String[] args) throws InterruptedException
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("TelephoneCallLogSpout", new TelephoneCallLogSpout(), 1);
        builder.setBolt("TelephoneCallTimeBolt", new TelephoneCallTimeBolt(), 4).globalGrouping("TelephoneCallLogSpout");
        builder.setBolt("TelephoneCallStatsBolt", new TelephoneCallStatsBolt(), 3).globalGrouping("TelephoneCallLogSpout");

        final Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(4);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TelephoneCallLogTopology", config, builder.createTopology());
        TimeUnit.SECONDS.sleep(100);

        cluster.killTopology("TelephoneCallLogTopology");
        cluster.shutdown();
    }
}
