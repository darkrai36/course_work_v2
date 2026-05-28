package com.sentinel.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserActivity {
    private String userId;
    private ActionType actionType;
    private String ipAddress;
    private long timestamp;

    public enum ActionType {
        LOGIN,
        LOGOUT,
        PAYMENT,
        PASSWORD_CHANGE,
        PROFILE_UPDATE,
        DEVICE_LINK
    }
}