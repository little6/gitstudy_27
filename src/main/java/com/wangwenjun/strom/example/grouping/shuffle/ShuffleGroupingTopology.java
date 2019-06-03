package com.wangwenjun.strom.example.grouping.shuffle;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleGroupingTopology
{
    private final static Logger LOG = LoggerFactory.getLogger(ShuffleGroupingTopology.class);

    public static void main(String[] args)
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ShuffleGroupingSpout", new ShuffleGroupingSpout(), 1);
        builder.setBolt("ShuffleGroupingBolt", new ShuffleGroupingBolt(), 2).shuffleGrouping("ShuffleGroupingSpout");
        builder.setBolt("ShuffleGroupingFinalBolt", new ShuffleGroupingFinalBolt(), 2).shuffleGrouping("ShuffleGroupingBolt");

        final Config conf = new Config();
        conf.setNumWorkers(3);
        try
        {
            StormSubmitter.submitTopology("ShuffleGroupingTopology", conf, builder.createTopology());
            LOG.warn("====================================================");
            LOG.warn("The Topology {} is submitted.", "ShuffleGroupingTopology");
            LOG.warn("====================================================");
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e)
        {
            e.printStackTrace();
        }
    }
}