package com.wangwenjun.strom.example.transaction.basic;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class TransactionalBatchSpout implements ITransactionalSpout<TransactionalMetaData>
{

    @Override
    public Coordinator<TransactionalMetaData> getCoordinator(Map conf, TopologyContext context)
    {
        return new TransactionalBatchCoordinator(context);
    }

    @Override
    public Emitter<TransactionalMetaData> getEmitter(Map conf, TopologyContext context)
    {
        return new TransactionalBatchEmitter(context);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("txID", "number"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }
}
