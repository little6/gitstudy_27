package com.wangwenjun.strom.example.trident.function;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class HelloTridentTopology
{
    private final static Logger LOG = LoggerFactory.getLogger(HelloTridentTopology.class);

    public static void main(String[] args)
            throws InterruptedException, InvalidTopologyException,
            AuthorizationException, AlreadyAliveException
    {
        boolean isRemoteMode = false;
        if (args.length > 0)
        {
            isRemoteMode = true;
        }

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("line"), 3,
                new Values("hello storm"),
                new Values("hello apache"),
                new Values("hello big data"),
                new Values("hello java"));
        spout.setCycle(true);

        final TridentTopology topology = new TridentTopology();
        final Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(false);

        topology.newStream("hellotx", spout).parallelismHint(1)
                .localOrShuffle()
                .map(new MyMapFunction(), new Fields("upper"))
                .parallelismHint(2)
                .partition(Grouping.fields(ImmutableList.of("upper")))
                .peek(input -> LOG.warn(input.getStringByField("upper")))
                .parallelismHint(3);
        if (isRemoteMode)
        {
            StormSubmitter.submitTopology("HelloTridentTopology", conf, topology.build());
            LOG.warn("The topology {} is submitted.", "HelloTridentTopology");
        } else
        {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HelloTridentTopology", conf, topology.build());
            TimeUnit.SECONDS.sleep(60);

            cluster.killTopology("HelloTridentTopology");
            cluster.shutdown();
        }
    }

    private static class MyMapFunction implements MapFunction
    {

        private final static Logger LOG = LoggerFactory.getLogger(MyMapFunction.class);

        @Override
        public Values execute(TridentTuple input)
        {
            String line = input.getStringByField("line");
            LOG.warn("The map function received: {}", line);
            return new Values(line.toUpperCase());
        }
    }
}
