package com.wangwenjun.strom.example.trident.action;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TelephoneCallLogTrident
{
    private final static Logger LOG = LoggerFactory.getLogger(WordCountTrident.class);

    public static void main(String[] args) throws InterruptedException
    {
        final FixedBatchSpout spout = new FixedBatchSpout(new Fields("caller", "callee", "duration"), 3,
                new Values("13193112345", "13193112346", 12),
                new Values("13193112346", "13193112347", 12),
                new Values("13193112347", "13193112348", 12),
                new Values("13193112347", "13193112348", 12),
                new Values("13193112347", "13193112349", 12)
        );

        final TridentTopology trident = new TridentTopology();
        trident.newStream("telephone-duration", spout).parallelismHint(1)
                .shuffle()
                .groupBy(new Fields("caller"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("duration"), new MySumReducer(), new Fields("duration-total"))
                .parallelismHint(5)
                .newValuesStream()
                .peek(input -> LOG.info("{}:{}", input.getFields(), input));


        trident.newStream("telephone-stats", spout).parallelismHint(1)
                .shuffle()
                .groupBy(new Fields("caller", "callee"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("times"))
                .parallelismHint(5)
                .newValuesStream()
                .peek(input -> LOG.info("{}:{}", input.getFields(), input));


        final Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("telephone", conf, trident.build());

        TimeUnit.MINUTES.sleep(2);
        cluster.killTopology("telephone");
        cluster.shutdown();
    }

    private final static class MySumReducer implements ReducerAggregator<Integer>
    {

        @Override
        public Integer init()
        {
            return 0;
        }

        @Override
        public Integer reduce(Integer curr, TridentTuple tuple)
        {
            return curr + tuple.getIntegerByField("duration");
        }
    }
}
