package com.wangwenjun.strom.example.grouping.custom;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CategoryGrouping implements CustomStreamGrouping
{
    private static final Map<String, Integer> categories = ImmutableMap.of(
            "JAVA", 0,
            "BIG_DATA", 1,
            "SCALA", 2,
            "C", 3
    );

    private List<Integer> targetTasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
                        List<Integer> targetTasks)
    {
        this.targetTasks = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values)
    {
        String category = (String) values.get(0);
        Integer index = categories.get(category);
        return Arrays.asList(this.targetTasks.get(index % this.targetTasks.size()));
    }
}
