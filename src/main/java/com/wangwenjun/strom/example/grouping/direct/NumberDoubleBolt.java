package com.wangwenjun.strom.example.grouping.direct;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NumberDoubleBolt extends BaseBasicBolt
{

    private final static Logger LOG = LoggerFactory.getLogger(NumberDoubleBolt.class);
    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.context = context;
        LOG.warn("TaskID:{}", context.getThisTaskId());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        int value = input.getIntegerByField("i");
        LOG.warn("taskID:{},instanceID:{},value:{}", context.getThisTaskId(), this, value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //no need implement
    }
}
