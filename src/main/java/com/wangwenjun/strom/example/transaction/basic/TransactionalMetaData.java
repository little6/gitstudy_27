package com.wangwenjun.strom.example.transaction.basic;

import java.io.Serializable;

public class TransactionalMetaData implements Serializable
{

    private int startIndex;
    private int endIndex;

    public TransactionalMetaData(int startIndex, int endIndex)
    {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public int getStartIndex()
    {
        return startIndex;
    }

    public void setStartIndex(int startIndex)
    {
        this.startIndex = startIndex;
    }

    public int getEndIndex()
    {
        return endIndex;
    }

    public void setEndIndex(int endIndex)
    {
        this.endIndex = endIndex;
    }

    @Override
    public String toString()
    {
        return "MetaData{" +
                "startIndex=" + startIndex +
                ", endIndex=" + endIndex +
                '}';
    }
}
