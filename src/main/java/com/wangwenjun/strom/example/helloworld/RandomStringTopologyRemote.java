package com.wangwenjun.strom.example.helloworld;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class RandomStringTopologyRemote
{
    public static void main(String[] args)
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RandomStringSpout", new RandomStringSpout());
        builder.setBolt("WrapStarBolt", new WrapStarBolt(), 4).shuffleGrouping("RandomStringSpout");
        builder.setBolt("WrapWellBolt", new WrapWellBolt(), 4).shuffleGrouping("RandomStringSpout");

        final Config conf = new Config();
        conf.setNumWorkers(3);

        try
        {
            StormSubmitter.submitTopology("RandomStringTopologyRemote", conf, builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e)
        {
            e.printStackTrace();
        }
    }
}