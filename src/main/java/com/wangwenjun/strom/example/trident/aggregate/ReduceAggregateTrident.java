package com.wangwenjun.strom.example.trident.aggregate;

import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * {@link AggregateTridentTest#testReducerAggregate} under the test package.
 */
public class ReduceAggregateTrident
{

    static class MyReducer implements ReducerAggregator<Integer>
    {

        @Override
        public Integer init()
        {
            return 0;
        }

        @Override
        public Integer reduce(Integer curr, TridentTuple tuple)
        {
            return curr + tuple.getInteger(0);
        }
    }
}
