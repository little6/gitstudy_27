package com.wangwenjun.strom.example.transaction.basic;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransactionalBatchCommitterBolt extends BaseTransactionalBolt implements ICommitter
{
    private final static Logger LOG = LoggerFactory.getLogger(TransactionalBatchCommitterBolt.class);

    private TopologyContext context;
    private BatchOutputCollector collector;
    private TransactionAttempt transactionAttempt;
    private List<Integer> watchList;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id)
    {
        this.context = context;
        this.collector = collector;
        this.transactionAttempt = id;
        this.watchList = new ArrayList<>();
    }

    @Override
    public void execute(Tuple tuple)
    {
        Integer number = tuple.getInteger(1);
        watchList.add(number);
        LOG.warn("txID:{},taskID:{},value:{}", transactionAttempt.getTransactionId(), context.getThisTaskId(), number);
    }

    @Override
    public void finishBatch()
    {
        LOG.warn("the batch commit [{}] done:{}", transactionAttempt.getTransactionId(), watchList);
        this.watchList.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {

    }
}
