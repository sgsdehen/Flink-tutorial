package com.ngt.streamingwithflink.util;

/**
 * @author ngt
 * @create 2021-05-17 21:25
 */
public class Alert {
    private String message;
    private long timestamp;

    public Alert() {
    }

    public Alert(String message, long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "(" + this.message + ", " + this.timestamp + ")";

    }
}