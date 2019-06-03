package com.wangwenjun.strom.example.telephone;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TelephoneCallTimeBolt extends BaseBasicBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(TelephoneCallTimeBolt.class);
    private Map<String, Double> summarizedMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.summarizedMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String caller = input.getStringByField("caller");
        Double duration = input.getDoubleByField("duration");
        if (summarizedMap.containsKey(caller))
        {
            summarizedMap.put(caller, summarizedMap.get(caller) + duration);
        } else
        {
            summarizedMap.put(caller, duration);
        }
    }

    @Override
    public void cleanup()
    {
        summarizedMap.forEach((caller, totalDuration) ->
                LOG.info("caller:{} ,total call out: {} minutes", caller, totalDuration)
        );
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // no need implement
    }
}
