package com.wangwenjun.strom.example.helloworld;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class WrapWellBolt extends BaseBasicBolt
{
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector)
    {
        final String value = tuple.getStringByField("stream");
//        debug("WrapWellBolt-execute:" + value, this);
        System.out.println("###### " + value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        // not need implement
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
//        debug("WrapWellBolt-prepare", this);
    }

    @Override
    public void cleanup()
    {
//        debug("WrapWellBolt-cleanup", this);
    }
}
