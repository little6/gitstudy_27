package com.wangwenjun.strom.example.grouping.all;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AllGroupingTopology
{
    private final static Logger LOG = LoggerFactory.getLogger(AllGroupingTopology.class);

    public static void main(String[] args)
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("StringAllGroupingSpout", new StringAllGroupingSpout(), 1);
        builder.setBolt("PreBolt", new PreBolt(), 1).shuffleGrouping("StringAllGroupingSpout");
        builder.setBolt("AllBolt1", new AllBolt1(), 2).allGrouping("PreBolt");
//        builder.setBolt("AllBolt2", new AllBolt2(), 1).fieldsGrouping("PreBolt", new Fields("upperName"));

        final Config conf = new Config();
        conf.setNumWorkers(4);
        try
        {
            StormSubmitter.submitTopology("AllGroupingTopology", conf, builder.createTopology());
            LOG.warn("====================================================");
            LOG.warn("The Topology {} is submitted.", "AllGroupingTopology");
            LOG.warn("====================================================");
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e)
        {
            e.printStackTrace();
        }
    }
}