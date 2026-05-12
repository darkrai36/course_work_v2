package com.sentinel.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Alert {
    private String userId;
    private String alertType;
    private String message;
    private long timestamp;
}
