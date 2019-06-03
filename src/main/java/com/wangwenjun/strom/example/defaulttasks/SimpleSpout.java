package com.wangwenjun.strom.example.defaulttasks;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.currentThread;

public class SimpleSpout extends BaseRichSpout
{
    private final static Logger LOG = LoggerFactory.getLogger(SimpleSpout.class);

    private SpoutOutputCollector collector;

    private TopologyContext context;

    private AtomicInteger ai;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        this.context = context;
        this.ai = new AtomicInteger();
        LOG.warn("SimpleSpout->open:hashcode:{}->thread:{},taskID:{}", this.hashCode(), currentThread(), context.getThisTaskId());

    }

    @Override
    public void nextTuple()
    {
        int i = this.ai.incrementAndGet();
        if (i <= 10)
        {
            LOG.warn("SimpleSpout->nextTuple:hashcode:{}->thread:{},taskID:{},Value:{}", this.hashCode(), currentThread(), context.getThisTaskId(), i);
            collector.emit(new Values(i));
        }

        try
        {
            TimeUnit.SECONDS.sleep(20);
        } catch (InterruptedException e)
        {
            //keep quietly
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
        LOG.warn("SimpleSpout->close:hashcode:{}->thread:{},taskID:{}", this.hashCode(), currentThread(), context.getThisTaskId());
    }
}
