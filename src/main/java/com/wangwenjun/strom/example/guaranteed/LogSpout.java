package com.wangwenjun.strom.example.guaranteed;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LogSpout extends BaseRichSpout
{
    private List<String> list;

    private SpoutOutputCollector collector;

    private int index;

    private Map<String, String> map;

    private static final Logger LOG = LoggerFactory.getLogger(LogSpout.class);

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        this.index = 0;
        this.list = Arrays.asList(
                "JAVA,IO",
                "JAVA,THREAD",
                "JAVA,COLLECTION",
                "JAVA,OO",
                "BIG_DATA,HADOOP",
                "BIG_DATA,STORM",
                "BIG_DATA,KAFKA",
                "BIG_DATA,FLUME",
                "SCALA,FUNCTIONAL",
                "SCALA,AKKA",
                "C,OS"
        );

        this.map = new HashMap<>();
    }

    @Override
    public void nextTuple()
    {
        while (index < list.size())
        {
            String messageID = UUID.randomUUID().toString();
            String value = list.get(index++);
            collector.emit(new Values(value), messageID);
            //记录发送出去的消息id和value的对应关系，防止后面处理失败需要重新传递，处理成功了，就从map中删除
            this.map.put(messageID, value);
        }
    }

    @Override
    public void ack(Object msgId)
    {
        LOG.info("Received the success ack messageID:{}.", msgId);
        //消息处理成功，就可以从map中删除了
        this.map.remove(msgId);
    }

    @Override
    public void fail(Object msgId)
    {
        LOG.error("Received the failed messageID:{}.", msgId);
        String value = this.map.get(msgId);
        //消息处理失败了，就可以根据之前map中记录的这条消息，重新进行发送一次
        collector.emit(new Values(value), msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("entity"));
    }
}
