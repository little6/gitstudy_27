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

public class TelephoneCallStatsBolt extends BaseBasicBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(TelephoneCallStatsBolt.class);
    private Map<Call, Integer> stats;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        this.stats = new HashMap<>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String caller = input.getStringByField("caller");
        String callee = input.getStringByField("callee");
        final Call call = new Call(caller, callee);
        if (stats.containsKey(call))
        {
            stats.put(call, stats.get(call) + 1);
        } else
        {
            stats.put(call, 1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //no need implements
    }

    @Override
    public void cleanup()
    {
        stats.forEach((call, times) ->
                LOG.info("call:{},total communication:{} times", call, times)
        );
    }

    private static class Call
    {
        private final String caller;
        private final String callee;

        private Call(String caller, String callee)
        {
            this.caller = caller;
            this.callee = callee;
        }

        @Override
        public String toString()
        {
            return "Call{" +
                    "caller='" + caller + '\'' +
                    ", callee='" + callee + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Call call = (Call) o;

            if (caller != null ? !caller.equals(call.caller) : call.caller != null) return false;
            return callee != null ? callee.equals(call.callee) : call.callee == null;
        }

        @Override
        public int hashCode()
        {
            int result = caller != null ? caller.hashCode() : 0;
            result = 31 * result + (callee != null ? callee.hashCode() : 0);
            return result;
        }
    }
}
