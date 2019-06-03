package com.wangwenjun.strom.example.helloworld;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.ThreadLocalRandom.current;

public class RandomStringSpout extends BaseRichSpout
{
    private final static Logger LOG = LoggerFactory.getLogger(RandomStringSpout.class);

    private static Map<Integer, String> map = new Hashtable<>();

    private SpoutOutputCollector collector;

    static
    {
        map.put(0, "KAFKA STREAMING");
        map.put(1, "APACHE NIFI");
        map.put(2, "APACHE FLINK");
        map.put(3, "APACHE STORM");
        map.put(4, "APACHE SPARK");
    }

    /**
     * 初始化方法，该组件实例化时，被调用一次
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector)
    {
        this.collector = spoutOutputCollector;
        LOG.info("spout opened.");
    }

    /**
     * 该方法是spout组件的核心逻辑
     * 不断的向下游发送tuple消息
     */
    @Override
    public void nextTuple()
    {
        String value = map.get(current().nextInt(5));
        collector.emit(new Values(value));
        try
        {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 声明该spout组件发送出去的tuple中的数据字段名称
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("stream"));
    }

    @Override
    public void close()
    {
    }
}