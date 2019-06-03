package com.wangwenjun.strom.example.state;

import org.apache.storm.trident.state.State;

import java.util.HashMap;
import java.util.Map;

public class DummyDB implements State
{
    private static Map<String, Technical> db = new HashMap<>();

    static
    {
        db.put("ALEX", new Technical("Storm", 1));
        db.put("ALLEN", new Technical("Spark", 2));
        db.put("KEVIN", new Technical("C++", 4));
        db.put("JACK", new Technical("JAVA", 5));
        db.put("JENNY", new Technical("Hadoop", 11));
    }

    @Override
    public void beginCommit(Long txid)
    {
    }

    @Override
    public void commit(Long txid)
    {
    }

    public Technical getTech(String name)
    {
        return db.get(name);
    }

    public void newTech(String name, Technical technical)
    {
        db.put(name, technical);
    }
}
