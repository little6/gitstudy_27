package com.wangwenjun.strom.example.transaction.basic;

import org.apache.storm.Config;
import org.apache.storm.transactional.TransactionalTopologyBuilder;

import java.util.concurrent.TimeUnit;

import static com.wangwenjun.strom.example.utils.Runner.runThenStop;

@SuppressWarnings("ALL")
public class TransactionalBatchTopology
{
    public static void main(String[] args) throws InterruptedException
    {
        final TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("test", "spout", new TransactionalBatchSpout(), 1);
        builder.setBolt("routing", new TransactionalBatchRoutingBolt(), 3).shuffleGrouping("spout");
        builder.setCommitterBolt("commit", new TransactionalBatchCommitterBolt(), 3).shuffleGrouping("routing");

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(10);
        config.setFallBackOnJavaSerialization(true);
        config.setMessageTimeoutSecs(100);

        config.setMaxSpoutPending(2);

        //System.out.println(config.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
        runThenStop("test", config, builder.buildTopology(), 5, TimeUnit.MINUTES);
    }
}
