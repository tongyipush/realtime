package com.atugigu.day05;

import java.sql.Timestamp;

public class UserBehavior {
    public String userId;
    public String itemId;
    public String categoryId;
    public String behaviorType;
    public Long timeStamp;

    public UserBehavior() {
    }

    public UserBehavior(String userId, String itemId, String categoryId, String behaviorType, Long timeStamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behaviorType = behaviorType;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", behaviorType='" + behaviorType + '\'' +
                ", timeStamp=" + new Timestamp(timeStamp) +
                '}';
    }
}
