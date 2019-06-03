package com.wangwenjun.strom.example.state;

import java.io.Serializable;

public class Technical implements Serializable
{

    private final String techName;

    private final int year;

    public Technical(String techName, int year)
    {
        this.techName = techName;
        this.year = year;
    }

    public String getTechName()
    {
        return techName;
    }

    public int getYear()
    {
        return year;
    }

    @Override
    public String toString()
    {
        return "Technical{" +
                "techName='" + techName + '\'' +
                ", year=" + year +
                '}';
    }
}
