package com.wangwenjun.strom.example.helloworld;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class WrapStarBolt extends BaseBasicBolt
{

    /**
     * bolt的核心业务逻辑
     * 上游来一条消息，该方法就会被调用一次
     * @param tuple
     * @param basicOutputCollector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector)
    {
        final String value = tuple.getStringByField("stream");
//        debug("WrapStarBolt-execute:" + value, this);

        System.out.println("****** " + value);
    }

    /**
     * 声明发送到下游的数据的字段名称(如果需要发送时)
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        //no need implement
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
//        debug("WrapStarBolt-prepare", this);
    }

    @Override
    public void cleanup()
    {
//        debug("WrapStarBolt-cleanup", this);
    }
}
