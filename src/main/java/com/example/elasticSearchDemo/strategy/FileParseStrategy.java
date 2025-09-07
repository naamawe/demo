package com.example.elasticSearchDemo.strategy;

public interface FileParseStrategy<T> {


    /**
     * 表名标识
     */
    String getTableName();

    /**
     * 将一行数据解析为实体对象
     * @param line 实体数据
     * @return 对应的实体对象
     */
    T parseLine(String line);
}
