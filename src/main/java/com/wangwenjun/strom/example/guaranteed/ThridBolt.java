package com.wangwenjun.strom.example.guaranteed;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.util.concurrent.ThreadLocalRandom.current;

public class ThridBolt extends BaseBasicBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(ThridBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String entity = input.getStringByField("entity");
        try
        {
            int x = 10 / current().nextInt(5);
            LOG.info("The entity:{} process success.", entity);
        } catch (Exception e)
        {
            LOG.error("The entity:{} process failed.",entity);
            throw new FailedException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //not need implement
    }
}
