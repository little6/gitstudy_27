package com.wangwenjun.strom.example.defaulttasks;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTopology
{
    private final static Logger LOG = LoggerFactory.getLogger(SimpleTopology.class);

    public static void main(String[] args)
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SimpleSpout", new SimpleSpout(), 4);

        builder.setBolt("SimpleBolt", new SimpleBolt(), 3).shuffleGrouping("SimpleSpout");

        final Config conf = new Config();
        conf.setNumWorkers(1);
        try
        {
            StormSubmitter.submitTopology("Default-Task", conf, builder.createTopology());
            LOG.warn("====================================================");
            LOG.warn("The Topology {} is submitted.", "Default-Task");
            LOG.warn("====================================================");
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e)
        {
            e.printStackTrace();
        }
    }
}