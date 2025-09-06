package com.example.elasticSearchDemo.entity;

import lombok.Getter;

import java.time.Instant;

@Getter
public class FailedObjectInfo {
    private final String prefix;
    private final String objectNameHint; // 可为空
    private final String failureReason;
    private final String errorMessage;
    private final Instant timestamp = Instant.now();

    public FailedObjectInfo(String prefix, String objectNameHint, String errorMessage) {
        this.prefix = prefix;
        this.objectNameHint = objectNameHint;
        this.failureReason = "metadata_parse_failed";
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return String.format("FailedObjectInfo{prefix='%s', hint='%s', reason='%s', error='%s'}",
                prefix, objectNameHint, failureReason, errorMessage);
    }
}
