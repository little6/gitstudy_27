package com.wangwenjun.strom.example.transaction.partitional;

import org.apache.storm.transactional.partitioned.IPartitionedTransactionalSpout;

public class TransactionalPartitionedCoordinator implements IPartitionedTransactionalSpout.Coordinator
{
    @Override
    public int numPartitions()
    {
        return 2;
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
