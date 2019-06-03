package com.wangwenjun.strom.example.drpc;

import org.apache.storm.Config;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManualDrpcClient
{
    private final static Logger LOG = LoggerFactory.getLogger(ManualDrpcClient.class);

    public static void main(String[] args)
    {
        Config conf = new Config();
        conf.put("storm.thrift.transport", "org.apache.storm.security.auth.plain.PlainSaslTransportPlugin");
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
        try (DRPCClient client = new DRPCClient(conf, "master", 3777))
        {
            String result = client.execute("add", "1,2");
            LOG.info("result:{}", result);
        } catch (TException e)
        {
            e.printStackTrace();
        }
    }
}
