package com.wangwenjun.strom.example.transaction.notransactional;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.wangwenjun.strom.example.utils.Runner.runThenStop;

public class NoTransactionalTopology
{

    private final static Logger LOG = LoggerFactory.getLogger(NoTransactionalTopology.class);

    public static void main(String[] args) throws InterruptedException
    {
        TridentTopology trident = new TridentTopology();
        trident.newStream("no-transactional", new NoTransactionalBatchSpout(5, Arrays.asList(
                new Values("Apache"), new Values("Storm"),
                new Values("Apache"), new Values("Storm"),
                new Values("Apache"), new Values("Storm"),
                new Values("Apache"), new Values("Storm"),
                new Values("Apache"), new Values("Storm")
        ), new Fields("name"))).parallelismHint(1)
                .groupBy(new Fields("name"))
                .each(new Fields("name"), new BaseFunction()
                {
                    @Override
                    public void execute(TridentTuple tuple, TridentCollector collector)
                    {
                        LOG.info(tuple.getString(0));
                        collector.emit(new Values(tuple.getString(0).toUpperCase()));
                    }
                }, new Fields("upperName")).toStream()
                .peek(input -> LOG.info("{}---{}", input.getFields(), input));

        Config config = new Config();
        config.setNumWorkers(3);
        config.setDebug(false);
        runThenStop("no-transactional", config, trident.build(), 1, TimeUnit.MINUTES);
    }

    public static class NoTransactionalBatchSpout implements IBatchSpout
    {

        private final static Logger LOG = LoggerFactory.getLogger(NoTransactionalBatchSpout.class);
        private final int batchSize;
        private final List<Values> values;
        private final Fields fields;
        private int index;
        private Map<Long, List<Values>> batchs;

        public NoTransactionalBatchSpout(int batchSize, List<Values> values, Fields fields)
        {
            if (batchSize < 1)
            {
                throw new IllegalArgumentException("The batch size is illegal.");
            }
            if (values == null || values.isEmpty())
            {
                throw new IllegalArgumentException("The values is illegal.");
            }

            if (fields == null || fields.toList().isEmpty())
            {
                throw new IllegalArgumentException("The fields is illegal.");
            }
            this.batchSize = batchSize;
            this.values = values;
            this.fields = fields;
        }

        @Override
        public void open(Map conf, TopologyContext context)
        {
            this.index = 0;
            this.batchs = new HashMap<>();
        }

        @Override
        public void emitBatch(long batchId, TridentCollector collector)
        {
            final List<Values> batch = new ArrayList<>();
            for (int i = 0; index < values.size() && i < batchSize; index++, i++)
            {
                batch.add(values.get(index));
            }

            if (!batch.isEmpty())
            {
                batchs.put(batchId, batch);
                batch.forEach(collector::emit);
            }
        }

        @Override
        public void ack(long batchId)
        {
            this.batchs.remove(batchId);
            LOG.info("The batch id: {} process successfully.", batchId);
        }

        @Override
        public void close()
        {
            // no need implement
        }

        @Override
        public Map<String, Object> getComponentConfiguration()
        {
            final Config config = new Config();
            config.setMaxTaskParallelism(1);
            return config;
        }

        @Override
        public Fields getOutputFields()
        {
            return fields;
        }
    }
}
