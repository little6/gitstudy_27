package com.wangwenjun.strom.example.wc;

import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class LineSpout extends BaseRichSpout
{

    private SpoutOutputCollector collector;

    private List<String> lines;

    private int index;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        this.lines = ImmutableList.of(
                "The storm for big data",
                "Java for big data",
                "storm is the real time compute framework",
                "scala is a functional language",
                "we like java and scala programming"
        );
    }

    @Override
    public void nextTuple()
    {
        while (index < lines.size())
        {
            String value = lines.get(index++);
            collector.emit(new Values(value));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("line"));
    }
}
