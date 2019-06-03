package com.wangwenjun.strom.example.parallel;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTopology
{
    //topology name
    //component prefix
    //workers
    //spout executor size(parallel hint)
    //spout task size
    //bolt  executor size(parallel hint)
    //bolt task size

    private final static Logger LOG = LoggerFactory.getLogger(SimpleTopology.class);

    public static void main(String[] args)
    {
        if (args.length < 7)
        {
            throw new IllegalArgumentException("The arguments is Illegal.");
        }

        final Options opts = Options.build(args);
        LOG.warn("Topology-Options:{}", opts);

        final String spoutName = opts.getPrefix() + "-SimpleSpout";
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(spoutName, new SimpleSpout(), opts.getSpoutParallelHint())
                .setNumTasks(opts.getSpoutTasks());

        builder.setBolt(opts.getPrefix() + "-SimpleBolt", new SimpleBolt(), opts.getBoltParallelHint())
                .setNumTasks(opts.getBoltTasks()).shuffleGrouping(spoutName);

        final Config conf = new Config();
        conf.setNumWorkers(opts.getWorkers());
        try
        {
            StormSubmitter.submitTopology(opts.getTopologyName(), conf, builder.createTopology());
            LOG.warn("====================================================");
            LOG.warn("The Topology {} is submitted.", opts.getTopologyName());
            LOG.warn("====================================================");
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e)
        {
            e.printStackTrace();
        }
    }

    private static class Options
    {
        private final String topologyName;
        private final String prefix;
        private final int workers;
        private final int spoutParallelHint;
        private final int spoutTasks;
        private final int boltParallelHint;
        private final int boltTasks;

        private Options(String topologyName, String prefix, int workers,
                        int spoutParallelHint, int spoutTasks,
                        int boltParallelHint, int boltTasks)
        {
            this.topologyName = topologyName;
            this.prefix = prefix;
            this.workers = workers;
            this.spoutParallelHint = spoutParallelHint;
            this.spoutTasks = spoutTasks;
            this.boltParallelHint = boltParallelHint;
            this.boltTasks = boltTasks;
        }

        static Options build(String[] args)
        {
            return new Options(args[0], args[1], Integer.parseInt(args[2]),
                    Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                    Integer.parseInt(args[5]), Integer.parseInt(args[6]));
        }

        String getTopologyName()
        {
            return topologyName;
        }

        String getPrefix()
        {
            return prefix;
        }

        int getWorkers()
        {
            return workers;
        }

        int getSpoutParallelHint()
        {
            return spoutParallelHint;
        }

        int getSpoutTasks()
        {
            return spoutTasks;
        }

        int getBoltParallelHint()
        {
            return boltParallelHint;
        }

        int getBoltTasks()
        {
            return boltTasks;
        }

        @Override
        public String toString()
        {
            return "Options{" +
                    "topologyName='" + topologyName + '\'' +
                    ", prefix='" + prefix + '\'' +
                    ", workers=" + workers +
                    ", spoutParallelHint=" + spoutParallelHint +
                    ", spoutTasks=" + spoutTasks +
                    ", boltParallelHint=" + boltParallelHint +
                    ", boltTasks=" + boltTasks +
                    '}';
        }
    }
}
