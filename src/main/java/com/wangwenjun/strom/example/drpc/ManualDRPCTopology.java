package com.wangwenjun.strom.example.drpc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ManualDRPCTopology
{
    public static void main(String[] args)
            throws InvalidTopologyException, AuthorizationException, AlreadyAliveException
    {
        TopologyBuilder builder = new TopologyBuilder();

        DRPCSpout spout = new DRPCSpout("add");
        builder.setSpout("drpc", spout);
        builder.setBolt("add", new AddCalculateBolt(), 3).shuffleGrouping("drpc");
        builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("add");

        Config conf = new Config();

        StormSubmitter.submitTopology("DRPC_ADD_TOPOLOGY", conf, builder.createTopology());
    }

    public static class AddCalculateBolt extends BaseBasicBolt
    {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector)
        {
            Object rtnInfo = input.getValue(1);
            String params = input.getString(0);
            String[] numbers = params.split(",");
            collector.emit(new Values(String.valueOf(Integer.parseInt(numbers[0]) + Integer.parseInt(numbers[1])), rtnInfo));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer)
        {
            declarer.declare(new Fields("result", "return-info"));
        }
    }
}
