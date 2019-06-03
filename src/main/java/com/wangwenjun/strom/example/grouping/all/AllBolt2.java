package com.wangwenjun.strom.example.grouping.all;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class AllBolt2 extends BaseBasicBolt
{

    private final static Logger LOG = LoggerFactory.getLogger(AllBolt2.class);

    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.context = context;
        LOG.warn("AllBolt2->prepare:hashcode:{},taskID:{}", this.hashCode(), context.getThisTaskId());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String name = input.getStringByField("upperName");
        LOG.warn("AllBolt2->execute:hashcode:{},taskID:{},RESULT:{}", this.hashCode(), context.getThisTaskId(), name);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //no need implements
    }
}
