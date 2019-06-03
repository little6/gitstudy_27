package com.wangwenjun.strom.example.transaction.opaque;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TransactionalOpaquePartitionedEmitter
        implements IOpaquePartitionedTransactionalSpout.Emitter<OpaquePartitionedMetaData>
{
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalOpaquePartitionedEmitter.class);
    private final TopologyContext context;
    private static final Map<Integer, List<Integer>> datas = ImmutableMap.of(
            0, Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            1, Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    );

    private static final int STEP = 5;

    private static final int PARTITIONS = 2;

    public TransactionalOpaquePartitionedEmitter(TopologyContext context)
    {
        this.context = context;
    }

    @Override
    public OpaquePartitionedMetaData emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector,
                                                        int partition, OpaquePartitionedMetaData lastPartitionMeta)
    {
        final OpaquePartitionedMetaData metaData;
        if (lastPartitionMeta == null)
        {
            metaData = new OpaquePartitionedMetaData(0, STEP);
        } else
        {
            metaData = new OpaquePartitionedMetaData(lastPartitionMeta.getEnd(), lastPartitionMeta.getEnd() + STEP);
        }
        LOG.warn("txId:{},taskId:{},preMetaData:{},currMetaData:{}", tx.getTransactionId(), context.getThisTaskId(),
                lastPartitionMeta, metaData);
        List<Integer> data = datas.get(partition);
        for (int i = metaData.getStart(); i < metaData.getEnd() && i < data.size(); i++)
        {
            Integer value = data.get(i);
            collector.emit(new Values(tx, value));
            LOG.warn("emit the value: {} in tx:{} use task: {}", value, tx.getTransactionId(), context.getThisTaskId());
        }
        return metaData;
    }

    @Override
    public int numPartitions()
    {
        return PARTITIONS;
    }

    @Override
    public void close()
    {
        // no need implement
    }
}
