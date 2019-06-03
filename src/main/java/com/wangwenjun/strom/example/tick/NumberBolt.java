package com.wangwenjun.strom.example.tick;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NumberBolt extends BaseBasicBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(NumberBolt.class);

    private List<Integer> list;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.list = new ArrayList<>();
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        final Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return conf;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        if (isTickTuple(input))
        {
            if (!list.isEmpty())
            {
                LOG.info("==========================");
                LOG.info("{}", list);
                LOG.info("==========================");
                this.list.clear();
            }
        } else
        {
            this.list.add(input.getIntegerByField("i"));
        }
    }

    private static boolean isTickTuple(Tuple tuple)
    {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //not need implement
    }
}
