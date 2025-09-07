package com.example.elasticSearchDemo.strategy.factory;


import com.example.elasticSearchDemo.strategy.FileParseStrategy;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class FileParseStrategyFactory {

    private final Map<String, FileParseStrategy<?>> STRATEGY_MAP = new HashMap<>();

    public FileParseStrategyFactory(List<FileParseStrategy<?>> strategies) {
        for (FileParseStrategy<?> strategy : strategies) {
            STRATEGY_MAP.put(strategy.getTableName(), strategy);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> FileParseStrategy<T> getStrategy(String tableName) {
        FileParseStrategy<?> strategy = STRATEGY_MAP.get(tableName);
        if (strategy == null) {
            throw new IllegalArgumentException("未找到对应的解析策略，tableName=" + tableName);
        }
        return (FileParseStrategy<T>) strategy;
    }
}
