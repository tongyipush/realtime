package com.atugigu.day02;

public class Alert {
    public String message;
    public Long timestamp;

    public Alert() {
    }

    public Alert(String message, Long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
