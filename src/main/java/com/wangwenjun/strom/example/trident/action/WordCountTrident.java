package com.wangwenjun.strom.example.trident.action;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.com.google.common.base.Splitter;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class WordCountTrident
{
    private final static Logger LOG = LoggerFactory.getLogger(WordCountTrident.class);

    public static void main(String[] args) throws InterruptedException
    {
        final FixedBatchSpout spout = new FixedBatchSpout(new Fields("line"), 3,
                new Values("The storm for big data"),
                new Values("Java for big data"),
                new Values("storm is the real time compute framework"),
                new Values("scala is a functional language"),
                new Values("we like java and scala programming")
        );

        final TridentTopology trident = new TridentTopology();
        trident.newStream("wc-stream", spout).parallelismHint(1)
                .shuffle()
                .each(new Fields("line"), new Split(), new Fields("word")).parallelismHint(3)
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(6)
                .newValuesStream()
                .peek(input -> LOG.info("{}:{}", input.getFields(), input));

        final Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wc", conf, trident.build());

        TimeUnit.MINUTES.sleep(2);
        cluster.killTopology("wc");
        cluster.shutdown();
    }

    private static class Split extends BaseFunction
    {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector)
        {
            String line = tuple.getString(0);
            Splitter.on(Pattern.compile("\\s+")).splitToList(line).forEach(
                    word -> collector.emit(new Values(word))
            );
        }
    }
}
