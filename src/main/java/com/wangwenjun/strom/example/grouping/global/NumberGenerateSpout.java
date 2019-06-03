package com.wangwenjun.strom.example.grouping.global;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class NumberGenerateSpout extends BaseRichSpout
{
    private SpoutOutputCollector collector;

    private AtomicInteger counter;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        this.counter = new AtomicInteger(0);
    }

    @Override
    public void nextTuple()
    {
        while (counter.get() < 10)
        {
            collector.emit(new Values(counter.getAndIncrement()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("i"));
    }
}