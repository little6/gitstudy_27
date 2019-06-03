package com.wangwenjun.strom.example.grouping.global;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalGroupingTopology
{
    private final static Logger LOG = LoggerFactory.getLogger(GlobalGroupingTopology.class);

    public static void main(String[] args)
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("NumberGenerateSpout", new NumberGenerateSpout(), 1);
        builder.setBolt("NumberDoubleBolt", new NumberDoubleBolt(), 2).shuffleGrouping("NumberGenerateSpout");
        builder.setBolt("NumberPrintBolt", new NumberPrintBolt(), 2).globalGrouping("NumberDoubleBolt");

        final Config conf = new Config();
        conf.setNumWorkers(4);
        try
        {
            StormSubmitter.submitTopology("GlobalGroupingTopology", conf, builder.createTopology());
            LOG.warn("====================================================");
            LOG.warn("The Topology {} is submitted.", "GlobalGroupingTopology");
            LOG.warn("====================================================");
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e)
        {
            e.printStackTrace();
        }
    }
}
