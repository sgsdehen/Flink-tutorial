package com.ngt.streamingwithflink.util;

/**
 * @author ngt
 * @create 2021-05-17 20:51
 */
public class SensorReading {
    public String id;
    public long timestamp;
    public double temperature;

    public SensorReading() {
    }

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.temperature + ")";
    }
}
