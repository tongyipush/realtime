package com.atugigu.day01;

public class SensorReading {
    public String id;
    public Double temperature;
    public Long timestamp;

    public SensorReading() {
    }

    public SensorReading(String id, Double temperature, Long timestamp) {
        this.id = id;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }



    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", temperature=" + temperature +
                ", timestamp=" + timestamp +
                '}';
    }
}
