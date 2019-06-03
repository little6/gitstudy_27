package com.wangwenjun.strom.example.transaction.opaque;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseOpaquePartitionedTransactionalSpout;
import org.apache.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class TransactionalOpaquePartitionedSpout extends BaseOpaquePartitionedTransactionalSpout<OpaquePartitionedMetaData>
{
    @Override
    public Emitter<OpaquePartitionedMetaData> getEmitter(Map conf, TopologyContext context)
    {
        return new TransactionalOpaquePartitionedEmitter(context);
    }

    @Override
    public Coordinator getCoordinator(Map conf, TopologyContext context)
    {
        return new IOpaquePartitionedTransactionalSpout.Coordinator()
        {
            @Override
            public boolean isReady()
            {
                return true;
            }

            @Override
            public void close()
            {
                //no need implement
            }
        };
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("txId", "number"));
    }
}
