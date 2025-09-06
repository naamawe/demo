package com.example.elasticSearchDemo.entity;

import lombok.Getter;

import java.time.Instant;
import java.util.*;

/**
 * 扫描结果封装类
 */
@Getter
public class ScanResult {
    private final Map<String, Instant> objects;
    private final boolean success;
    private final List<String> failedPrefixes;
    private final List<FailedObjectInfo> failedObjects;

    public ScanResult(Map<String, Instant> objects, boolean success,
                      List<String> failedPrefixes, List<FailedObjectInfo> failedObjects) {
        this.objects = Collections.unmodifiableMap(new HashMap<>(objects));
        this.success = success;
        this.failedPrefixes = Collections.unmodifiableList(new ArrayList<>(failedPrefixes));
        this.failedObjects = Collections.unmodifiableList(new ArrayList<>(failedObjects));
    }
}
