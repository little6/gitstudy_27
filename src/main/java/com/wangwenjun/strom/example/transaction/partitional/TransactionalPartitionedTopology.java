package com.wangwenjun.strom.example.transaction.partitional;

import org.apache.storm.Config;
import org.apache.storm.transactional.TransactionalTopologyBuilder;

import java.util.concurrent.TimeUnit;

import static com.wangwenjun.strom.example.utils.Runner.runThenStop;

@SuppressWarnings("ALL")
public class TransactionalPartitionedTopology
{
    public static void main(String[] args) throws InterruptedException
    {
        final TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("test", "spout", new TransactionalPartitionedSpout(), 1);
        builder.setBolt("routing", new TransactionalPartitionedRoutingBolt(), 3).shuffleGrouping("spout");
        builder.setCommitterBolt("commit", new TransactionalPartitionedCommitterBolt(), 3).shuffleGrouping("routing");

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(10);
        config.setFallBackOnJavaSerialization(true);
        config.setMessageTimeoutSecs(100);
        config.setMaxSpoutPending(2);
        runThenStop("test", config, builder.buildTopology(), 5, TimeUnit.MINUTES);
    }
}
