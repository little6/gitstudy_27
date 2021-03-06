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

public class ShuffleRepartitionTrident
{
    private final static Logger LOG = LoggerFactory.getLogger(ShuffleRepartitionTrident.class);

    public static void main(String[] args) throws InterruptedException
    {
        final FixedBatchSpout spout = new FixedBatchSpout(new Fields("x", "y", "z"), 3,
                new Values(1, 2, 3),
                new Values(4, 5, 6),
                new Values(7, 8, 9),
                new Values(10, 11, 12)
        );

        spout.setCycle(false);
        final Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(false);

        TridentTopology topology = new TridentTopology();
        topology.newStream("ShuffleRepartitionStream", spout).parallelismHint(1)
                .shuffle().peek(tridentTuple -> LOG.info("{}", tridentTuple)).parallelismHint(2);


        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("ShuffleRepartitionTrident", conf, topology.build());
        TimeUnit.SECONDS.sleep(30);
        cluster.killTopology("ShuffleRepartitionTrident");
        cluster.shutdown();
    }
}
