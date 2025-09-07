package com.example.elasticSearchDemo.strategy.Impl;

import com.example.elasticSearchDemo.entity.IndexData;
import com.example.elasticSearchDemo.strategy.FileParseStrategy;
import org.springframework.stereotype.Component;

@Component
public class IndexDataParseStrategy implements FileParseStrategy<IndexData> {

    @Override
    public String getTableName() {
        return "index_data";
    }

    @Override
    public IndexData parseLine(String line) {
        if (line == null || line.trim().isEmpty()){
            return null;
        }

        String[] parts = line.split(",");
        if (parts.length < 3){
            throw new IllegalArgumentException("非法数据行: " + line);
        }

        return IndexData.builder()
                .id(Long.parseLong(parts[0]))
                .name(parts[1])
                .age(Integer.parseInt(parts[2]))
                .build();
    }
}
