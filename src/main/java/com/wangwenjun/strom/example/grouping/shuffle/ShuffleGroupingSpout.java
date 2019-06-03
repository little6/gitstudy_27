package com.wangwenjun.strom.example.grouping.shuffle;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.currentThread;

public class ShuffleGroupingSpout extends BaseRichSpout
{
    private final static Logger LOG = LoggerFactory.getLogger(ShuffleGroupingSpout.class);

    private SpoutOutputCollector collector;

    private TopologyContext context;

    private AtomicInteger ai;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        this.context = context;
        this.ai = new AtomicInteger();
        LOG.warn("ShuffleGroupingSpout->open:hashcode:{},taskID:{}", this.hashCode(), currentThread(), context.getThisTaskId());

    }

    @Override
    public void nextTuple()
    {
        int i = this.ai.incrementAndGet();
        if (i <= 10)
        {
            LOG.warn("ShuffleGroupingSpout->nextTuple:hashcode:{},taskID:{},Value:{}", this.hashCode(), context.getThisTaskId(), i);
            collector.emit(new Values(i));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("i"));
    }

    @Override
    public void close()
    {
        LOG.warn("ShuffleGroupingSpout->close:hashcode:{},taskID:{}", this.hashCode(), context.getThisTaskId());
    }
}
