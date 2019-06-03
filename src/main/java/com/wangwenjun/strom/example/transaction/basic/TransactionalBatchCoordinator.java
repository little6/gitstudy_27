package com.wangwenjun.strom.example.transaction.basic;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.transactional.ITransactionalSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

public class TransactionalBatchCoordinator implements ITransactionalSpout.Coordinator<TransactionalMetaData>
{
    private final static Logger LOG = LoggerFactory.getLogger(TransactionalBatchCoordinator.class);

    private final TopologyContext context;

    public TransactionalBatchCoordinator(TopologyContext context)
    {
        this.context = context;
    }

    @Override
    public TransactionalMetaData initializeTransaction(BigInteger txid, TransactionalMetaData prevMetadata)
    {
        LOG.warn("txID:{},prevMetadata:{},taskID:{}", txid, prevMetadata, context.getThisTaskId());
        if (prevMetadata == null)
        {
            return new TransactionalMetaData(0, 5);
        } else
        {
            return new TransactionalMetaData(prevMetadata.getEndIndex(), prevMetadata.getEndIndex() + 5);
        }
    }

    @Override
    public boolean isReady()
    {
        return true;
    }

    @Override
    public void close()
    {
    }
}
