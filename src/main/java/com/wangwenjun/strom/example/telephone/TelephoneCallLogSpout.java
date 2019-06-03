package com.wangwenjun.strom.example.telephone;

import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.ThreadLocalRandom.current;

public class TelephoneCallLogSpout extends BaseRichSpout
{
    private SpoutOutputCollector collector;
    private List<String> phoneList;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        this.phoneList = ImmutableList.of(
                "13193112345",
                "13293112345",
                "13393112345",
                "13493112345",
                "15193112345"
        );
    }

    @Override
    public void nextTuple()
    {
        int size = phoneList.size();
        String caller = phoneList.get(current().nextInt(size));
        String callee = phoneList.get(current().nextInt(size));
        while (caller.equals(callee))
        {
            callee = phoneList.get(current().nextInt(size));
        }
        double duration = current().nextDouble(10);

        collector.emit(new Values(caller, callee, duration));

        try
        {
            TimeUnit.SECONDS.sleep(current().nextInt(5));
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("caller", "callee", "duration"));
    }
}
