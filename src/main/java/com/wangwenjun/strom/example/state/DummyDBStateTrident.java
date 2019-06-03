package com.wangwenjun.strom.example.state;

import org.apache.storm.Config;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.wangwenjun.strom.example.utils.Runner.runThenStop;
import static java.util.stream.Collectors.toList;

public class DummyDBStateTrident
{
    private final static Logger LOG = LoggerFactory.getLogger(DummyDBStateTrident.class);

    public static void main(String[] args) throws InterruptedException
    {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("name"), 3,
                new Values("Alex"), new Values("Allen"),
                new Values("Kevin"), new Values("Jack"),
                new Values("Jenny"), new Values("Eachur"), new Values("Sebastian")
        );

        DummyDBFactory dummyDBFactory = new DummyDBFactory();
        TridentTopology trident = new TridentTopology();
        TridentState state = trident.newStaticState(dummyDBFactory);
        trident.newStream("test", spout).parallelismHint(1).localOrShuffle()
                .each(new Fields("name"), new UpperFunction(), new Fields("capitalized"))
                .parallelismHint(3)
                .partitionPersist(dummyDBFactory, new Fields("capitalized"), new TechUpdater(), new Fields("name"))
                .newValuesStream()
                .stateQuery(state, new Fields("name"), new QueryFunction(), new Fields("tech", "year"))
                .parallelismHint(3)
                .localOrShuffle()
                .peek(input -> LOG.info("{}-{}", input.getFields(), input));
        final Config conf = new Config();
        conf.setNumWorkers(5);
        conf.setDebug(false);
        runThenStop("test", conf, trident.build(), 1, TimeUnit.MINUTES);

    }

    private static class UpperFunction extends BaseFunction
    {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector)
        {
            collector.emit(new Values(tuple.getString(0).toUpperCase()));
        }
    }

    private static class TechUpdater extends BaseStateUpdater<DummyDB>
    {

        @Override
        public void updateState(DummyDB state, List<TridentTuple> tuples, TridentCollector collector)
        {
            tuples.forEach(tuple ->
            {
                String name = tuple.getString(0);
                Technical technical = state.getTech(name);
                if (technical == null)
                {
                    technical = new Technical("Scala", 10);
                    state.newTech(name, technical);
                }

                collector.emit(new Values(name, technical.getTechName(), technical.getYear()));
            });
        }
    }

    private static class QueryFunction extends BaseQueryFunction<DummyDB, Technical>
    {

        @Override
        public List<Technical> batchRetrieve(DummyDB state, List<TridentTuple> args)
        {
            return args.stream().map(tuple -> state.getTech(tuple.getString(0))).collect(toList());
        }

        @Override
        public void execute(TridentTuple tuple, Technical result, TridentCollector collector)
        {
            collector.emit(new Values(result.getTechName(), result.getYear()));
        }
    }

}
