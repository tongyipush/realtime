package com.atugigu.day03;

import java.sql.Timestamp;

public class MaxMinTemp {
    public String id;
    public Double minTemp;
    public Double maxTemp;
    public Long startTime;
    public Long endTime;

    public MaxMinTemp() {
    }

    public MaxMinTemp(String id, Double minTemp, Double maxTemp, Long startTime, Long endTime) {
        this.id = id;
        this.minTemp = minTemp;
        this.maxTemp = maxTemp;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "MaxMinTemp{" +
                "id='" + id + '\'' +
                ", minTemp=" + minTemp +
                ", maxTemp=" + maxTemp +
                ", startTime=" + new Timestamp(startTime) +
                ", endTime=" + new Timestamp(endTime) +
                '}';
    }
}
