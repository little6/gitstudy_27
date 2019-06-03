package com.wangwenjun.strom.example.guaranteed;

import com.wangwenjun.strom.example.grouping.custom.LogPrintBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FirstBolt extends BaseBasicBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(FirstBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String entity = input.getStringByField("entity");
        collector.emit(new Values(entity));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("entity"));
    }
}