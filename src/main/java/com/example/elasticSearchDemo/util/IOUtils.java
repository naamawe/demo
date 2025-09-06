package com.example.elasticSearchDemo.util;

import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class IOUtils {

    //读取服务器上的文件
    //全量读取
    public File[] fullGetFileFromPath(String basePath, String tableName, String date) {

        String filePath = basePath +"/" + tableName + "/" + date;

        File folder = new File(filePath);
        File[] files = folder.listFiles();

        return files;
    }

}
