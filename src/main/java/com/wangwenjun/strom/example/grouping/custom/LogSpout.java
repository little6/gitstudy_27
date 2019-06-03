package com.wangwenjun.strom.example.grouping.custom;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LogSpout extends BaseRichSpout
{
    private List<String> list;

    private SpoutOutputCollector collector;

    private int index;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        this.index = 0;
        this.list = Arrays.asList(
                "JAVA,IO",
                "JAVA,THREAD",
                "JAVA,COLLECTION",
                "JAVA,OO",
                "BIG_DATA,HADOOP",
                "BIG_DATA,STORM",
                "BIG_DATA,KAFKA",
                "BIG_DATA,FLUME",
                "SCALA,FUNCTIONAL",
                "SCALA,AKKA",
                "C,OS"
        );
    }

    @Override
    public void nextTuple()
    {
        while (index < list.size())
        {
            collector.emit(new Values(list.get(index++)));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("entity"));
    }
}
