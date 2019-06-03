package com.wangwenjun.strom.example.grouping.shuffle;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ShuffleGroupingBolt extends BaseBasicBolt
{

    private final static Logger LOG = LoggerFactory.getLogger(ShuffleGroupingBolt.class);

    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.context = context;
        LOG.warn("ShuffleGroupingBolt->prepare:hashcode:{},taskID:{}", this.hashCode(), context.getThisTaskId());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        Integer i = input.getIntegerByField("i");
        collector.emit(new Values(i * 10));
        LOG.warn("ShuffleGroupingBolt->execute:hashcode:{},taskID:{},value:{}", this.hashCode(), context.getThisTaskId(), i);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("result"));
    }

    @Override
    public void cleanup()
    {
        LOG.warn("ShuffleGroupingBolt->cleanup:hashcode:{},taskID:{}", this.hashCode(), context.getThisTaskId());
    }
}
