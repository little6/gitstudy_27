package com.wangwenjun.strom.example.grouping.direct;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectGroupingTopology
{
    private final static Logger LOG = LoggerFactory.getLogger(DirectGroupingTopology.class);

    public static void main(String[] args)
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("NumberGenerateSpout", new NumberGenerateSpout(), 1);
        builder.setBolt("NumberDoubleBolt", new NumberDoubleBolt(), 2).directGrouping("NumberGenerateSpout");
        //builder.setBolt("NumberDoubleBolt", new NumberDoubleBolt(), 2).noneGrouping("NumberGenerateSpout");
        //builder.setBolt("NumberDoubleBolt", new NumberDoubleBolt(), 2).localOrShuffleGrouping("NumberGenerateSpout");



        final Config conf = new Config();
        conf.setNumWorkers(3);
        try
        {
            StormSubmitter.submitTopology("DirectGroupingTopology", conf, builder.createTopology());
            LOG.warn("====================================================");
            LOG.warn("The Topology {} is submitted.", "DirectGroupingTopology");
            LOG.warn("====================================================");
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e)
        {
            e.printStackTrace();
        }
    }
}
