package com.wangwenjun.strom.example.trident.aggregate;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * please execute the unit test one by one(or comments the other test cases and keep only one is running)
 */
public class AggregateTridentTest
{

    private final static Logger LOG = LoggerFactory.getLogger(AggregateTridentTest.class);
    private FixedBatchSpout spout;
    private FixedBatchSpout spout2;

    @Before
    public void setUp()
    {
        this.spout = new FixedBatchSpout(new Fields("name", "age"), 3,
                new Values("Alex", 33),
                new Values("Alice", 31),
                new Values("Eachur", 29),
                new Values("Jack", 32),
                new Values("Leo", 34),
                new Values("Yuki", 18),
                new Values("Andy", 38)
        );
        this.spout.setCycle(false);

        this.spout2 = new FixedBatchSpout(new Fields("name", "age"), 4,
                new Values("Alex", 33),
                new Values("Alex", 31),
                new Values("Eachur", 29),
                new Values("Jack", 32),
                new Values("Jack", 38),
                new Values("Yuki", 34),
                new Values("Yuki", 18)

        );
        this.spout2.setCycle(false);
    }

    @Test
    public void testPartitionAggregate() throws InterruptedException
    {
        final TridentTopology trident = new TridentTopology();
        trident.newStream("testPartitionAggregate", spout).parallelismHint(2)
                .shuffle()
                .partitionAggregate(new Fields("age", "name"), new Count(), new Fields("count")).parallelismHint(2)
                .each(new Fields("count"), new Debug());
        this.submitTopologyThenKill("testPartitionAggregate", trident.build());
    }


    @Test
    public void testAggregate() throws InterruptedException
    {
        final TridentTopology trident = new TridentTopology();
        trident.newStream("testAggregate", spout).parallelismHint(2)
                .aggregate(new Fields("age", "name"), new Count(), new Fields("count")).parallelismHint(5)
                .each(new Fields("count"), new Debug());
        this.submitTopologyThenKill("testAggregate", trident.build());
    }

    @Test
    public void testReducerAggregate() throws InterruptedException
    {
        final TridentTopology trident = new TridentTopology();
        trident.newStream("testReducerAggregate", spout).parallelismHint(2)
                .partitionBy(new Fields("name"))
                .aggregate(new Fields("age", "name"), new ReduceAggregateTrident.MyReducer(), new Fields("sum")).parallelismHint(5)
                .each(new Fields("sum"), new Debug());
        this.submitTopologyThenKill("testReducerAggregate", trident.build());
    }

    @Test
    public void testAggregatorTrident() throws InterruptedException
    {
        final TridentTopology trident = new TridentTopology();
        trident.newStream("testAggregatorTrident", spout).parallelismHint(2)
                .partitionBy(new Fields("name"))
                .aggregate(new Fields("age"), new AggregatorTrident.SumAsAggregator(), new Fields("sum")).parallelismHint(5)
                .each(new Fields("sum"), new Debug());
        this.submitTopologyThenKill("testAggregatorTrident", trident.build());
    }

    @Test
    public void testCombinerAggregatorTrident() throws InterruptedException
    {
        final TridentTopology trident = new TridentTopology();
        trident.newStream("testCombinerAggregatorTrident", spout).parallelismHint(2)
                .partitionBy(new Fields("name"))
                .aggregate(new Fields("age"), new CombinerAggregatorTrident.MyCount(), new Fields("sum")).parallelismHint(5)
                .each(new Fields("sum"), new Debug());
        this.submitTopologyThenKill("testCombinerAggregatorTrident", trident.build());
    }

    @Test
    public void testPersistenceAggregateTrident() throws InterruptedException
    {
        final TridentTopology trident = new TridentTopology();
        trident.newStream("testPersistenceAggregateTrident", spout).parallelismHint(1)
                .partitionBy(new Fields("name"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("name"), new Count(), new Fields("count"))
                .parallelismHint(4)
                .newValuesStream()
                .peek(tuple -> LOG.info("{}", tuple));
        this.submitTopologyThenKill("testPersistenceAggregateTrident", trident.build());
    }

    @Test
    public void testChainAggTrident() throws InterruptedException
    {
        final TridentTopology trident = new TridentTopology();
        trident.newStream("testPersistenceAggregateTrident", spout).parallelismHint(1)
                .partitionBy(new Fields("name"))
                .chainedAgg()
                .aggregate(new Fields("name"), new Count(), new Fields("count"))
                .aggregate(new Fields("age"), new Sum(), new Fields("sum"))
//                .aggregate(new Fields("age"), new Count(), new Fields("count2"))
                .chainEnd()
                .peek(tuple -> LOG.info("{},{}", tuple.getFields(), tuple));
        this.submitTopologyThenKill("testPersistenceAggregateTrident", trident.build());
    }

    @Test
    public void testGroupByTrident() throws InterruptedException
    {
        final TridentTopology trident = new TridentTopology();
        trident.newStream("testGroupByTrident", spout2).parallelismHint(1)
                .partitionBy(new Fields("name"))
                .groupBy(new Fields("name"))
                .aggregate(new Count(), new Fields("count"))
                .peek(tuple -> LOG.info("{},{}", tuple.getFields(), tuple));
        this.submitTopologyThenKill("testPersistenceAggregateTrident", trident.build());
    }

    private void submitTopologyThenKill(String name, StormTopology topology) throws InterruptedException
    {
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, createConf(), topology);
        TimeUnit.MINUTES.sleep(1);
        cluster.killTopology(name);
        cluster.shutdown();
    }

    private Config createConf()
    {
        final Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(false);
        return conf;
    }
}
