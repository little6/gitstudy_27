package com.wangwenjun.strom.example.grouping.global;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class NumberDoubleBolt extends BaseBasicBolt
{
    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        int value = input.getIntegerByField("i");
        collector.emit(new Values(value, 10));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("i", "constant"));
    }
}
