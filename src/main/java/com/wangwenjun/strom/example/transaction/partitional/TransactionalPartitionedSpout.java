package com.wangwenjun.strom.example.transaction.partitional;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BasePartitionedTransactionalSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class TransactionalPartitionedSpout extends BasePartitionedTransactionalSpout<PartitionedMetaData>
{

    @Override
    public Coordinator getCoordinator(Map conf, TopologyContext context)
    {
        return new TransactionalPartitionedCoordinator();
    }

    @Override
    public Emitter<PartitionedMetaData> getEmitter(Map conf, TopologyContext context)
    {
        return new TransactionalPartitionedEmitter(context);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("txId", "number"));
    }
}
