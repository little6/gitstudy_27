package com.wangwenjun.strom.example.transaction.partitional;

import java.io.Serializable;

public class PartitionedMetaData implements Serializable
{
    private final int start;
    private final int end;

    public PartitionedMetaData(int start, int end)
    {
        this.start = start;
        this.end = end;
    }

    public int getStart()
    {
        return start;
    }

    public int getEnd()
    {
        return end;
    }

    @Override
    public String toString()
    {
        return "MetaData{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}
