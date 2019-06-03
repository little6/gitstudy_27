package com.wangwenjun.strom.example.guaranteed;

import com.wangwenjun.strom.example.grouping.custom.LogPrintBolt;
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

/**
 * 继承与baseBasicBolt时的消息确认：正常处理就OK，不需要调用ack
 * 如果不正常处理，就抛出FailedException就可以了，不需要调用fail方法
 */
public class TestBolt extends BaseBasicBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(LogPrintBolt.class);

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
            LOG.info("The entity:{} process success.");
        } catch (Exception e)
        {
            throw new FailedException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //not need implement
    }
}
