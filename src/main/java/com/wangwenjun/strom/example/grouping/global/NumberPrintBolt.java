package com.wangwenjun.strom.example.grouping.global;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NumberPrintBolt extends BaseBasicBolt
{

    private final static Logger LOG = LoggerFactory.getLogger(NumberPrintBolt.class);

    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.context = context;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        int i = input.getIntegerByField("i");
        int constant = input.getIntegerByField("constant");
        LOG.info("taskID:{},instanceID:{},i:{},constant:{}", context.getThisTaskId(), this, i, constant);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //no need implements
    }
}
