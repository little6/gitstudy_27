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
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * 测试处理消息超时，调用fail方法的测试
 */
public class TestTimeOutBolt extends BaseBasicBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(LogPrintBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        try
        {
            TimeUnit.SECONDS.sleep(4);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //not need implement
    }
}
