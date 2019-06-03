package com.wangwenjun.strom.example.trident.aggregate;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * {@link AggregateTridentTest#testCombinerAggregatorTrident} under the test package.
 */
public class CombinerAggregatorTrident
{
    static class MyCount implements CombinerAggregator<Integer>
    {

        @Override
        public Integer init(TridentTuple tuple)
        {
            return tuple.getInteger(0);
        }

        @Override
        public Integer combine(Integer val1, Integer val2)
        {
            return val1 + val2;
        }

        @Override
        public Integer zero()
        {
            return 0;
        }
    }

}
