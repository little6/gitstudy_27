package com.wangwenjun.strom.example.transaction.partitional;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TransactionalPartitionedEmitter implements IPartitionedTransactionalSpout.Emitter<PartitionedMetaData>
{
    private final static Logger LOG = LoggerFactory.getLogger(TransactionalPartitionedEmitter.class);

    private final TopologyContext context;

    private static final int STEP = 5;

    private static final Map<Integer, List<Integer>> partitionData = ImmutableMap.of(
            0, Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            1, Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    );

    public TransactionalPartitionedEmitter(TopologyContext context)
    {
        this.context = context;
    }

    @Override
    public PartitionedMetaData emitPartitionBatchNew(TransactionAttempt tx, BatchOutputCollector collector,
                                                     int partition, PartitionedMetaData lastPartitionMeta)
    {

        LOG.info("txID:{},partition:{},metaData:{},taskID:{}", tx.getTransactionId(), partition, lastPartitionMeta, context.getThisTaskId());
        final PartitionedMetaData metaData;
        if (null == lastPartitionMeta)
        {
            metaData = new PartitionedMetaData(0, STEP);
        } else
        {
            metaData = new PartitionedMetaData(lastPartitionMeta.getEnd(), lastPartitionMeta.getEnd() + STEP);
        }
        this.emitPartitionBatch(tx, collector, partition, metaData);
        return metaData;
    }

    @Override
    public void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector,
                                   int partition, PartitionedMetaData partitionMeta)
    {
        List<Integer> data = partitionData.get(partition);
        for (int i = partitionMeta.getStart(); i < partitionMeta.getEnd() && i < data.size(); i++)
        {
            collector.emit(new Values(tx, data.get(i)));
            LOG.warn("-emit,txID:{},taskID:{},metaData:{},value:{}", tx.getTransactionId(), context.getThisTaskId(), partitionMeta, data.get(i));
        }
    }

    @Override
    public void close()
    {

    }
}
