package com.wangwenjun.strom.example.grouping.custom;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogPrintBolt extends BaseBasicBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(LogPrintBolt.class);

    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.context = context;
        LOG.info("prepare:{}", context.getThisTaskId());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String category = input.getStringByField("category");
        String item = input.getStringByField("item");
        LOG.info("category:{},item:{},taskID:{}", category, item, context.getThisTaskId());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //not need implement
    }
}
