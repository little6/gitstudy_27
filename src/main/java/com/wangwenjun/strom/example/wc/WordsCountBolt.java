package com.wangwenjun.strom.example.wc;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class WordsCountBolt extends BaseBasicBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(WordsCountBolt.class);

    private Map<String, Integer> countMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.countMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String word = input.getStringByField("word");
        if (countMap.containsKey(word))
        {
            countMap.put(word, countMap.get(word) + 1);
        } else
        {
            countMap.put(word, 1);
        }
    }

    @Override
    public void cleanup()
    {
        countMap.forEach((key, value) -> LOG.info("word:{},total:{}", key, value));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {

    }
}
