package com.wangwenjun.strom.example.grouping.shuffle;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ShuffleGroupingFinalBolt extends BaseBasicBolt
{

    private final static Logger LOG = LoggerFactory.getLogger(ShuffleGroupingFinalBolt.class);

    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.context = context;
        LOG.warn("ShuffleGroupingFinalBolt->prepare:hashcode:{},taskID:{}", this.hashCode(), context.getThisTaskId());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        Integer i = input.getIntegerByField("result");
        LOG.warn("ShuffleGroupingFinalBolt->execute:hashcode:{},taskID:{},RESULT:{}", this.hashCode(), context.getThisTaskId(), i);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //no need implements
    }

    @Override
    public void cleanup()
    {
        LOG.warn("ShuffleGroupingFinalBolt->cleanup:hashcode:{},taskID:{}", this.hashCode(), context.getThisTaskId());
    }
}
