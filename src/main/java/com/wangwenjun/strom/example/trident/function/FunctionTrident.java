package com.wangwenjun.strom.example.trident.function;

import org.apache.storm.Config;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wangwenjun.strom.example.utils.Runner.runThenStop;

public class FunctionTrident
{
    private final static Logger LOG = LoggerFactory.getLogger(FunctionTrident.class);

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
        topology.newStream("function", spout).parallelismHint(1)
                .localOrShuffle()
                .peek(input -> LOG.info("==={},{},{},{}", input.getInteger(0), input.getInteger(1), input.getInteger(2), input.getInteger(3)))
                .parallelismHint(2)
                .localOrShuffle()
                .each(new Fields("a", "b"), new SumFunction(), new Fields("sum"))
                .parallelismHint(2)
                .localOrShuffle()
                .peek(input -> LOG.info("+++{},{},{},{},{}", input.getIntegerByField("a"), input.getIntegerByField("b"), input.getIntegerByField("c"), input.getIntegerByField("d"), input.getIntegerByField("sum")))
                .parallelismHint(1);

        runThenStop("FunctionTrident", conf, topology.build(), 30);
    }

    private static class SumFunction extends BaseFunction
    {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector)
        {
            Integer a = tuple.getIntegerByField("a");
            Integer b = tuple.getIntegerByField("b");
            collector.emit(new Values(a + b));
        }
    }
}
