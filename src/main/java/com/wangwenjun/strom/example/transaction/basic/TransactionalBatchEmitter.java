package com.wangwenjun.strom.example.transaction.basic;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

public class TransactionalBatchEmitter implements ITransactionalSpout.Emitter<TransactionalMetaData>
{

    private final static Logger LOG = LoggerFactory.getLogger(TransactionalBatchEmitter.class);

    private final TopologyContext context;

    private static final int[] values = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    private int index = 0;

    public TransactionalBatchEmitter(TopologyContext context)
    {
        this.context = context;
    }

    @Override
    public void emitBatch(TransactionAttempt tx, TransactionalMetaData metaData, BatchOutputCollector collector)
    {
        for (int i = metaData.getStartIndex(); i < metaData.getEndIndex() && i < values.length; i++)
        {
            int value = values[i];
            LOG.warn("Tx:{},metaData:{},task:{},value:{}", tx.getTransactionId(), metaData, context.getThisTaskId(), value);
            collector.emit(new Values(tx, value));
        }
    }

    @Override
    public void cleanupBefore(BigInteger txid)
    {
    }

    @Override
    public void close()
    {
    }
}
