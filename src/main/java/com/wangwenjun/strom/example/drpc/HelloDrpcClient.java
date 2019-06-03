package com.wangwenjun.strom.example.drpc;

import org.apache.storm.Config;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloDrpcClient
{
    private final static Logger LOG = LoggerFactory.getLogger(HelloDrpcClient.class);

    public static void main(String[] args) throws TException
    {
        Config conf = new Config();
        conf.put("storm.thrift.transport", "org.apache.storm.security.auth.plain.PlainSaslTransportPlugin");
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
        DRPCClient client = new DRPCClient(conf, "master", 3777);
        String result = client.execute("echo", "alex");
        LOG.info("result:{}", result);
    }
}
