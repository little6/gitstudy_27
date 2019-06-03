package com.wangwenjun.strom.example.grouping.fields;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StringGenerateSpout extends BaseRichSpout
{
    private final static Logger LOG = LoggerFactory.getLogger(StringGenerateSpout.class);

    private SpoutOutputCollector collector;

    private TopologyContext context;

    private List<String> data;

    private int index;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        this.context = context;
        this.index = 0;
        this.data = Arrays.asList("Alex", "Alex", "Alex", "Alex", "Alex", "Alex", "Alex", "Alex", "Wang", "Wang");
        LOG.warn("StringGenerateSpout->open:hashcode:{},taskID:{}", this.hashCode(), context.getThisTaskId());
    }

    @Override
    public void nextTuple()
    {

        if (index < data.size())
        {
            String name = data.get(index++);
            LOG.warn("StringGenerateSpout->nextTuple:hashcode:{},taskID:{},Value:{}", this.hashCode(), context.getThisTaskId(), name);
            collector.emit(new Values(name));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("name"));
    }

    @Override
    public void close()
    {
        LOG.warn("StringGenerateSpout->close:hashcode:{},taskID:{}", this.hashCode(), context.getThisTaskId());
    }
}
