package com.wangwenjun.strom.example.trident.function;

import org.apache.storm.Config;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wangwenjun.strom.example.utils.Runner.runThenStop;

public class FilterTrident
{
    private final static Logger LOG = LoggerFactory.getLogger(FilterTrident.class);

    public static void main(String[] args) throws InterruptedException
    {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b", "c", "d"), 3,
                new Values(1, 4, 7, 10),
                new Values(1, 1, 3, 11),
                new Values(2, 2, 7, 1),
                new Values(2, 5, 7, 2));
        spout.setCycle(false);

        final Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(false);

        TridentTopology topology = new TridentTopology();
        topology.newStream("Filter", spout).parallelismHint(1)
                .localOrShuffle()
                .peek(input -> LOG.info("==={},{},{},{}", input.getInteger(0), input.getInteger(1),
                        input.getInteger(2), input.getInteger(3))
                ).parallelismHint(2)
                .localOrShuffle()
                .each(new Fields("a", "b"), new CheckEvenSumFilter())
                .parallelismHint(2)
                .localOrShuffle()
                .peek(input -> LOG.info("+++{},{},{},{}", input.getIntegerByField("a"), input.getIntegerByField("b"),
                        input.getIntegerByField("c"), input.getIntegerByField("d"))
                ).parallelismHint(1);

        runThenStop("FilterTrident", conf, topology.build(), 30);
    }

    private static class CheckEvenSumFilter extends BaseFilter
    {

        @Override
        public boolean isKeep(TridentTuple tuple)
        {
            Integer a = tuple.getIntegerByField("a");
            Integer b = tuple.getIntegerByField("b");
            return ((a + b) % 2 == 0);
        }
    }
}
