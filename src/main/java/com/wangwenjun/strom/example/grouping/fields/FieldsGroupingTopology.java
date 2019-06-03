package com.wangwenjun.strom.example.grouping.fields;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldsGroupingTopology
{
    private final static Logger LOG = LoggerFactory.getLogger(FieldsGroupingTopology.class);

    public static void main(String[] args)
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("StringGenerateSpout", new StringGenerateSpout(), 1);
        builder.setBolt("StringUpperBolt", new StringUpperBolt(), 2).shuffleGrouping("StringGenerateSpout");
        builder.setBolt("FieldsGroupingFinalBolt", new FieldsGroupingFinalBolt(), 2).fieldsGrouping("StringUpperBolt", new Fields("upperName"));

        final Config conf = new Config();
        conf.setNumWorkers(3);
        try
        {
            StormSubmitter.submitTopology("ShuffleFieldsTopology", conf, builder.createTopology());
            LOG.warn("====================================================");
            LOG.warn("The Topology {} is submitted.", "ShuffleFieldsTopology");
            LOG.warn("====================================================");
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e)
        {
            e.printStackTrace();
        }
    }
}