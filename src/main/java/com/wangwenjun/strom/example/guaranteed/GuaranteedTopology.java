package com.wangwenjun.strom.example.guaranteed;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

public class GuaranteedTopology
{
    public static void main(String[] args) throws InterruptedException
    {
        final TopologyBuilder builder = new TopologyBuilder();

        //1.test process failed.
        //builder.setSpout("LogSpout", new LogSpout(), 1);
        //builder.setBolt("TestBolt", new TestBolt(), 4).localOrShuffleGrouping("LogSpout");

        //2. test time out
        //builder.setSpout("LogSpout", new LogSpout(), 1);
        //builder.setBolt("TestTimeOutBolt", new TestTimeOutBolt(), 4).localOrShuffleGrouping("LogSpout");

        //3. test fully processed
//        builder.setSpout("LogSpout", new LogSpout(), 1);
//        builder.setBolt("FirstBolt", new FirstBolt(), 2).localOrShuffleGrouping("LogSpout");
//        builder.setBolt("SecondBolt", new SecondBolt(), 2).localOrShuffleGrouping("FirstBolt");

        //3. spout tuple to 2 bolts
        builder.setSpout("LogSpout", new LogSpout(), 1);
        builder.setBolt("SecondBolt", new SecondBolt(), 2).localOrShuffleGrouping("LogSpout");
        builder.setBolt("ThirdBolt", new ThridBolt(), 2).localOrShuffleGrouping("LogSpout");

        final Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(false);
        //设置每条消息处理的超时时间
        conf.setMessageTimeoutSecs(2);

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("GuaranteedTopology", conf, builder.createTopology());

        TimeUnit.SECONDS.sleep(30);
        cluster.killTopology("GuaranteedTopology");
        cluster.shutdown();


    }
}
