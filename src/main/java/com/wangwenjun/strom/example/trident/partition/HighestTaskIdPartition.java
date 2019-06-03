package com.wangwenjun.strom.example.trident.partition;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HighestTaskIdPartition implements CustomStreamGrouping
{
    private int destTaskID;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
                        List<Integer> targetTasks)
    {
        ArrayList<Integer> tasks = new ArrayList<>(targetTasks);
        Collections.sort(tasks);
        this.destTaskID = tasks.get(tasks.size() - 1);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values)
    {
        return Arrays.asList(destTaskID);
    }
}
