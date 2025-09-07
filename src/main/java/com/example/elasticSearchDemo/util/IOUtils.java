package com.example.elasticSearchDemo.util;

import com.example.elasticSearchDemo.strategy.FileParseStrategy;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

@Component
public class IOUtils {

    //读取服务器上的文件
    //全量读取
    public static File[] fullGetFileFromPath(String basePath, String tableName, String date) {

        String filePath = basePath +"/" + tableName + "/" + date;

        File folder = new File(filePath);

        return folder.listFiles();
    }

    /**
     * 通用解析文件
     * @param files    要解析的文件数组
     * @param strategy 解析策略
     * @param <T>      实体类型
     * @return 实体对象集合
     */
    public static <T> List<T> parseFiles(File[] files, FileParseStrategy<T> strategy) {
        List<T> result = Lists.newArrayList();

        if (files == null) {
            return result;
        }

        for (File file : files){
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                boolean isFirstLine = true;

                while ((line = br.readLine()) != null){

                    if (isFirstLine) {
                        isFirstLine = false;
                        continue;
                    }

                    T obj = strategy.parseLine(line);
                    if (obj != null) {
                        result.add(obj);
                    }
                }

            } catch (IOException e) {
                throw new RuntimeException("读取文件失败: " + file.getAbsoluteFile(), e);
            }
        }

        return result;
    }
}
