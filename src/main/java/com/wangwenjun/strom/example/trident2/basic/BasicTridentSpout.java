package com.wangwenjun.strom.example.trident2.basic;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicTridentSpout implements ITridentSpout<BasicTridentSpout.MetaData>
{

    private final List<Values> values;

    private final int batch;

    private final Fields fields;

    public BasicTridentSpout(List<Values> values, int batch, Fields fields)
    {
        this.values = values;
        this.batch = batch;
        this.fields = fields;
    }

    @Override
    public BatchCoordinator<MetaData> getCoordinator(String txStateId, Map conf, TopologyContext context)
    {
        return new BasicTridentCoordinator(context, batch);
    }

    @Override
    public Emitter<MetaData> getEmitter(String txStateId, Map conf, TopologyContext context)
    {
        return new BasicTridentEmitter(context, values);
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields()
    {
        List<String> realFields = new ArrayList<>();
        realFields.add("txId");
        realFields.add(this.fields.get(0));
        return new Fields(realFields);
    }

    static class BasicTridentEmitter implements ITridentSpout.Emitter<MetaData>
    {
        private static final Logger LOG = LoggerFactory.getLogger(BasicTridentEmitter.class);

        private final TopologyContext context;

        private final List<Values> values;

        BasicTridentEmitter(TopologyContext context, List<Values> values)
        {
            this.context = context;
            this.values = values;
        }

        @Override
        public void emitBatch(TransactionAttempt tx, MetaData metaData, TridentCollector collector)
        {
            LOG.warn("txID:{},metaData:{},taskID:{}", tx.getTransactionId(), metaData, context.getThisTaskId());
            for (int i = metaData.getStart(); i < metaData.getEnd() && i < values.size(); i++)
            {
                List<Object> pendingValues = new ArrayList<>();
                pendingValues.add(tx);
                pendingValues.add(values.get(i).get(0));
                collector.emit(pendingValues);
            }
        }

        @Override
        public void success(TransactionAttempt tx)
        {
            LOG.warn("the txid:{} emit completed.", tx.getTransactionId());
        }

        @Override
        public void close()
        {

        }
    }

    static class BasicTridentCoordinator implements ITridentSpout.BatchCoordinator<MetaData>
    {

        private static final Logger LOG = LoggerFactory.getLogger(BasicTridentCoordinator.class);

        private final TopologyContext context;

        private final int batch;

        private final Map<Long, MetaData> metaDataMap;

        BasicTridentCoordinator(TopologyContext context, int batch)
        {
            this.context = context;
            this.batch = batch;
            this.metaDataMap = new HashMap<>();
        }


        @Override
        public MetaData initializeTransaction(long txid, MetaData prevMetadata, MetaData currMetadata)
        {
            LOG.warn("txID:{},prevMetadata:{},currMetadata:{},taskID:{}", txid, prevMetadata, currMetadata, context.getThisTaskId());
            final MetaData metaData;
            if (null == prevMetadata)
            {
                metaData = new MetaData(0, batch);
            } else
            {
                metaData = new MetaData(prevMetadata.getEnd(), prevMetadata.getEnd() + batch);
            }
            LOG.warn("the new metaData:{}", metaData);
            this.metaDataMap.put(txid, metaData);
            return metaData;
        }

        @Override
        public void success(long txid)
        {
            LOG.warn("the txID:{} process completed and metaData:{}.", txid, metaDataMap.get(txid));
        }

        @Override
        public boolean isReady(long txid)
        {
            return true;
        }

        @Override
        public void close()
        {
        }
    }

    static class MetaData implements Serializable
    {

        private final int start;
        private final int end;

        private MetaData(int start, int end)
        {
            this.start = start;
            this.end = end;
        }

        int getStart()
        {
            return start;
        }

        int getEnd()
        {
            return end;
        }

        @Override
        public String toString()
        {
            return "MetaData{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }
    }
}
