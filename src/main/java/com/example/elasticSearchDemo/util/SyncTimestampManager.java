package com.example.elasticSearchDemo.util;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Component
public class SyncTimestampManager {
    
    // 文件路径常量
    public static final String SYNC_FILE_ES = "last_sync_es.txt";
    public static final String SYNC_FILE_MINIO = "last_sync_minio.txt";

    /**
     * 读取上次同步时间戳
     * 
     * @return 上次同步时间（Instant格式）
     *         文件不存在或读取失败时返回1小时前的时间
     */
    public static Instant readLastSyncTime(String SYNC_FILE) {
        Path path = Paths.get(SYNC_FILE);
        
        try {
            // 检查文件是否存在
            if (Files.exists(path)) {
                // 读取文件内容
                String timestampStr = Files.readString(path).trim();
                
                // 解析为Instant对象
                return Instant.parse(timestampStr);
            }
        } catch (Exception e) {
            System.err.println("读取同步时间失败: " + e.getMessage());
        }
        
        // 默认返回1小时前（首次运行或文件不存在）
        return Instant.now().minus(1, ChronoUnit.HOURS);
    }
    
    /**
     * 保存当前同步时间戳
     * 
     * @param timestamp 要保存的时间戳（Instant格式）
     */
    public static void saveLastSyncTime(String SYNC_FILE, Instant timestamp) {
        Path path = Paths.get(SYNC_FILE);
        
        try {
            // 将Instant转换为字符串
            String timestampStr = timestamp.toString();
            
            // 写入文件（覆盖原有内容）
            Files.writeString(path, timestampStr);
            
            System.out.println("同步时间已更新: " + timestampStr);
        } catch (IOException e) {
            System.err.println("保存同步时间失败: " + e.getMessage());
        }
    }
}