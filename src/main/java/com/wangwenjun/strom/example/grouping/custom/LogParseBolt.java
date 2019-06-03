package com.wangwenjun.strom.example.grouping.custom;

import org.apache.storm.shade.com.google.common.base.Splitter;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;

public class LogParseBolt extends BaseBasicBolt
{
    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String entity = input.getStringByField("entity");
        List<String> items = Splitter.on(",").splitToList(entity);
        collector.emit(new Values(items.get(0), items.get(1)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("category", "item"));
    }
}
