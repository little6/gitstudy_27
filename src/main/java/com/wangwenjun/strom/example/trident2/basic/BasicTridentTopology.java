package com.wangwenjun.strom.example.trident2.basic;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.wangwenjun.strom.example.utils.Runner.runThenStop;

public class BasicTridentTopology
{

    private static final Logger LOG = LoggerFactory.getLogger(BasicTridentTopology.class);

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException
    {
        TridentTopology trident = new TridentTopology();
        trident.newStream("BasicTridentTopologyStream", new BasicTridentSpout(Arrays.asList(
                new Values("alex1"), new Values("alex2"),
                new Values("alex3"), new Values("alex4"),
                new Values("alex5"), new Values("alex6"),
                new Values("alex7"), new Values("alex8"),
                new Values("alex9"), new Values("alex10")
        ), 5, new Fields("name"))).parallelismHint(1)
                .localOrShuffle()
                .each(new Fields("name"), new UpperNameFunction(), new Fields("upperName"))
                .parallelismHint(1)
                .peek(input -> LOG.warn("{}-{}", input.getFields(), input));

        final Config conf = new Config();
        conf.setMaxSpoutPending(2);
        conf.setDebug(false);
        conf.setNumWorkers(3);
        runThenStop("test", conf, trident.build(), 1, TimeUnit.MINUTES);

//        StormSubmitter.submitTopology("BasicTridentTopologyStream", conf, trident.build());
    }

    private static class UpperNameFunction extends BaseFunction
    {

        private static boolean error = true;

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector)
        {

            String data = tuple.getString(0);
            if (data.equals("alex3") && error)
            {
                error = false;
                throw new FailedException();
            }
            collector.emit(new Values(data.toUpperCase()));
        }
    }
}
