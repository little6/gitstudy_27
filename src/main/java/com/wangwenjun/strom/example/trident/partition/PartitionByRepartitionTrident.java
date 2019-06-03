package com.wangwenjun.strom.example.trident.partition;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PartitionByRepartitionTrident
{
    private final static Logger LOG = LoggerFactory.getLogger(PartitionByRepartitionTrident.class);

    public static void main(String[] args) throws InterruptedException
    {
        final FixedBatchSpout spout = new FixedBatchSpout(new Fields("name", "age"), 3,
                new Values("alex", 33),
                new Values("jack", 30),
                new Values("alex", 50),
                new Values("jack", 33),
                new Values("jack", 40)
        );

        spout.setCycle(false);
        final Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(false);

        TridentTopology topology = new TridentTopology();
        topology.newStream("PartitionByRepartitionStream", spout).parallelismHint(1)
                .partitionBy(new Fields("name")).peek(tridentTuple -> LOG.info("{}", tridentTuple)).parallelismHint(3);


        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("PartitionByRepartitionTrident", conf, topology.build());
        TimeUnit.SECONDS.sleep(30);
        cluster.killTopology("PartitionByRepartitionTrident");
        cluster.shutdown();
    }
}
