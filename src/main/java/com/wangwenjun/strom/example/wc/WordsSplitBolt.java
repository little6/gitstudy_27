package com.wangwenjun.strom.example.wc;

import org.apache.storm.shade.com.google.common.base.Splitter;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.regex.Pattern;

public class WordsSplitBolt extends BaseBasicBolt
{
    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String line = input.getStringByField("line");
        Splitter.on(Pattern.compile("\\s+"))
                .splitToList(line)
                .forEach(word -> collector.emit(new Values(word)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word"));
    }
}
