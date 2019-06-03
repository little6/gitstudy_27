package com.wangwenjun.strom.example.trident.aggregate;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * {@link AggregateTridentTest#testAggregatorTrident} under the test package.
 */
public class AggregatorTrident
{

    static class SumAsAggregator extends BaseAggregator<SumAsAggregator.State>
    {
        @Override
        public State init(Object batchId, TridentCollector collector)
        {
            return new State();
        }

        @Override
        public void aggregate(State state, TridentTuple tuple, TridentCollector collector)
        {
            state.count = state.count + tuple.getInteger(0);
        }

        @Override
        public void complete(State state, TridentCollector collector)
        {
            collector.emit(new Values(state.count));
        }

        static class State
        {
            long count = 0;
        }
    }
}
