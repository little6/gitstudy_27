package com.wangwenjun.strom.example.parallel;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.lang.Thread.currentThread;

public class SimpleBolt extends BaseBasicBolt
{

    private final static Logger LOG = LoggerFactory.getLogger(SimpleBolt.class);

    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.context = context;
        LOG.warn("SimpleBolt->prepare:hashcode:{}->thread:{},taskID:{}", this.hashCode(), currentThread(), context.getThisTaskId());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        Integer i = input.getIntegerByField("i");
        LOG.warn("SimpleBolt->execute:hashcode:{}->thread:{},taskID:{},value:{}", this.hashCode(), currentThread(), context.getThisTaskId(),i);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //no need implement
    }

    @Override
    public void cleanup()
    {
        LOG.warn("SimpleBolt->cleanup:hashcode:{}->thread:{},taskID:{}", this.hashCode(), currentThread(), context.getThisTaskId());
    }
}
