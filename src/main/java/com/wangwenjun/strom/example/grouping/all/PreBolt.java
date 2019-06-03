package com.wangwenjun.strom.example.grouping.all;

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

public class PreBolt extends BaseBasicBolt
{

    private final static Logger LOG = LoggerFactory.getLogger(PreBolt.class);

    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.context = context;
        LOG.warn("PreBolt->prepare:hashcode:{},taskID:{}", this.hashCode(), context.getThisTaskId());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String name = input.getStringByField("name");
        LOG.warn("PreBolt->execute:hashcode:{},taskID:{},value:{}", this.hashCode(), context.getThisTaskId(), name);
        collector.emit(new Values(name.toUpperCase()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("upperName"));
    }
}
