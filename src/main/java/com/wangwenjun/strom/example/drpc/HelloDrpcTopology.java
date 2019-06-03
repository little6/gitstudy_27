package com.wangwenjun.strom.example.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class HelloDrpcTopology
{
    private final static Logger LOG = LoggerFactory.getLogger(HelloDrpcTopology.class);

    public static void main(String[] args)
            throws InvalidTopologyException, AuthorizationException, AlreadyAliveException
    {
        boolean isRemote = !(args.length == 0);
        final LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("echo");

        builder.addBolt(new EchoBolt(), 1);
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);
        if (isRemote)
        {
            StormSubmitter.submitTopologyWithProgressBar("HelloDrpcTopology", conf, builder.createRemoteTopology());
            LOG.warn("The topology {} is submitted by remote way.", "HelloDrpcTopology");
        } else
        {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HelloDrpcTopology", conf, builder.createLocalTopology(drpc));
            Arrays.asList("alex", "wang", "storm", "apache").forEach(item ->
            {
                String result = drpc.execute("echo", item);
                LOG.warn("result--->{}", result);
            });

            cluster.killTopology("HelloDrpcTopology");
            cluster.shutdown();
            drpc.shutdown();
        }
    }

    public static class EchoBolt extends BaseBasicBolt
    {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector)
        {
            LOG.warn("tuple:{}", input.getFields());
            Object id = input.getValue(0);
            String name = input.getString(1);
            collector.emit(new Values(id, "echo:" + name));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer)
        {
            declarer.declare(new Fields("id", "value"));
        }
    }
}
