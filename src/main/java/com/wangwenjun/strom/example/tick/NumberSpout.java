package com.wangwenjun.strom.example.tick;

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

public class NumberSpout extends BaseRichSpout
{

    private static final Logger LOG = LoggerFactory.getLogger(NumberSpout.class);

    private AtomicInteger inc;

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.inc = new AtomicInteger();
        this.collector = collector;
    }

    @Override
    public void nextTuple()
    {
        int value = inc.getAndIncrement();
        collector.emit(new Values(value), value);
        LOG.info("spout:{}", value);
        try
        {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId)
    {
        LOG.info("The message {} processed completed", msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("i"));
    }
}
