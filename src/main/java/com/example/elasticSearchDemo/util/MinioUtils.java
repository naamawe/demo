package com.example.elasticSearchDemo.util;

import com.example.elasticSearchDemo.config.MinioConfig;
import com.example.elasticSearchDemo.entity.FailedObjectInfo;
import com.example.elasticSearchDemo.entity.ScanResult;
import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;


@Component
public class MinioUtils {

    private static final Logger log = LoggerFactory.getLogger(MinioUtils.class);
    @Autowired
    private MinioClient minioClient;

    @Autowired
    private MinioConfig configuration;

    /**
     * @Description 判断bucket是否存在
     * @param name  名字
     * @return {@link Boolean }
     */
    public Boolean existBucket(String name) throws Exception {
       return minioClient.bucketExists(BucketExistsArgs.builder().bucket(name).build());
    }

    /**
     * @Description      创建存储bucket
     * @param bucketName 存储bucket名称
     * @return {@link Boolean }
     */
    public Boolean makeBucket(String bucketName) {
        try {
            minioClient.makeBucket(MakeBucketArgs.builder()
                    .bucket(bucketName)
                    .build());
        } catch (Exception e) {
            System.out.println("创建桶失败");
            return false;
        }
        return true;
    }

    /**
     * @Description      删除存储bucket
     * @param bucketName 存储bucket名称
     * @return {@link Boolean }
     */
    public Boolean removeBucket(String bucketName) {
        try {
            minioClient.removeBucket(RemoveBucketArgs.builder()
                    .bucket(bucketName)
                    .build());
        } catch (Exception e) {
            System.out.println("删除桶失败");
            return false;
        }
        return true;
    }

    /**
     * @Description      列出所有存储桶
     * @return           储存bucket列表
     * @throws Exception 捕获异常
     */
    public List<Bucket> listBuckets() throws Exception {
        return minioClient.listBuckets();
    }

    /**
     * 构造基于当前日期的前缀，格式：year/month/day/
     * 例如：2025/9/05/
     */
    private String buildDatePrefix() {
        LocalDate now = LocalDate.now();
        return String.format("%d/%02d/%02d/",
                now.getYear(),
                now.getMonthValue(),
                now.getDayOfMonth());
    }

    /**
     * @Description      上传文件
     * @param file       文件
     * @param bucketName 桶名称
     * @return           文件名
     * @throws Exception 抛出异常
     */
    public String upload(MultipartFile file, String bucketName) throws Exception{
        if (bucketName == null) {
            bucketName = "test";
        }
        if (!existBucket(bucketName)) {
            makeBucket(bucketName);
        }

        String fileName = file.getOriginalFilename();
        if (fileName == null || fileName.isEmpty()){
            throw new Exception("文件名不能为空");
        }

        //获取拓展名
        String extension = fileName.substring(fileName.lastIndexOf("."));
        //生成唯一文件名
        String objectName = UUID.randomUUID().toString().replaceAll("-", "") + extension;

        String datePrefix = buildDatePrefix();
        objectName = datePrefix + objectName;

        minioClient.putObject(
                PutObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .stream(file.getInputStream(), file.getSize(), -1)
                        .contentType(file.getContentType())
                        .build()
        );
        return objectName;
    }

    /**
     * @Description      删除文件
     * @param bucketName 桶名称
     * @param objectName 文件名
     * @throws Exception 抛出异常
     */
    public void removeFile(String bucketName, String objectName) throws Exception {
        minioClient.removeObject(
                RemoveObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build());
    }

    /**
     * @Description      获取文件url
     * @param bucketName 桶名称
     * @param objectName 文件名
     * @return           文件路径
     * @throws Exception 抛出异常
     */
    public String getFileUrl(String bucketName, String objectName) throws Exception {

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("response-content-type", "text/plain; charset=utf-8");

        return minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .method(Method.GET)
                        .expiry(7, TimeUnit.DAYS)
                        .extraQueryParams(queryParams)
                        .build());
    }

    /**
     * @Description             获取最后修改时间大于指定时间的对象列表
     * @param bucketName        存储桶名称
     * @param lastModifiedAfter 最后修改时间的起始点
     * @return                  符合条件的对象列表
     */
    public ScanResult listObjectsModifiedAfter(String bucketName, Instant lastModifiedAfter) {
        return listObjectsModifiedAfter(bucketName, lastModifiedAfter, ZoneId.systemDefault());
    }

    /**
     * @Description             获取最后修改时间大于指定时间的对象列表
     * @param bucketName        存储桶名称
     * @param lastModifiedAfter 最后修改时间的起始点
     * @return                  符合条件的对象列表
     */
    public ScanResult listObjectsModifiedAfter(String bucketName, Instant lastModifiedAfter, ZoneId zoneId) {
        Map<String, Instant> modifiedObjects = new HashMap<>();
        Set<String> seen = new HashSet<>();
        List<String> failedPrefixes = new ArrayList<>();
        List<FailedObjectInfo> failedObjects = new ArrayList<>();

        List<String> prefixes = generateDatePrefixes(lastModifiedAfter, zoneId);
        int maxRetries = 3;

        long start = System.currentTimeMillis();
        log.info("开始扫描 bucket={}, lastModifiedAfter={}, 时区={}, 共 {} 个 prefix",
                bucketName, lastModifiedAfter, zoneId, prefixes.size());

        for (String prefix : prefixes) {
            boolean success = false;
            int attempt = 0;

            while (!success && attempt < maxRetries) {
                attempt++;
                try {
                    log.info("第 {} 次尝试扫描 prefix: {}", attempt, prefix);

                    Iterable<Result<Item>> results = minioClient.listObjects(
                            ListObjectsArgs.builder()
                                    .bucket(bucketName)
                                    .prefix(prefix)
                                    .recursive(true)
                                    .build()
                    );

                    //遍历已加载的结果
                    for (Result<Item> result : results) {
                        String objectNameHint = "unknown";
                        try {
                            Item item = result.get();
                            objectNameHint = item.objectName();

                            // 去重
                            if (!seen.add(objectNameHint)) {
                                continue;
                            }

                            Instant modifiedInstant = item.lastModified().toInstant();
                            if (modifiedInstant.isAfter(lastModifiedAfter)) {
                                modifiedObjects.put(objectNameHint, modifiedInstant);
                            }
                        } catch (Exception e) {
                            failedObjects.add(new FailedObjectInfo(prefix, objectNameHint, e.getMessage()));
                            log.warn("跳过无法解析的对象元数据 (prefix={}, object={}): {}", prefix, objectNameHint, result, e);
                        }
                    }

                    log.debug("prefix '{}' 扫描成功", prefix);
                    success = true;

                } catch (Exception e) {
                    if (attempt >= maxRetries) {
                        log.error("经过 {} 次重试，prefix {} 仍扫描失败，将被加入 failedPrefixes", maxRetries, prefix, e);
                        failedPrefixes.add(prefix);
                    } else {
                        log.info("第 {} 次扫描 prefix {} 失败，准备重试: {}", attempt, prefix, e.getMessage());
                        try {
                            Thread.sleep(1000L * attempt);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            log.warn("重试等待被中断，继续下一个 prefix");
                        }
                    }
                }
            }
        }

        boolean overallSuccess = failedPrefixes.isEmpty();
        long duration = System.currentTimeMillis() - start;

        log.info("完成扫描，共找到 {} 个符合条件的对象，" +
                        "失败 prefix 数: {}，部分失败对象数: {}，耗时 {}ms",
                modifiedObjects.size(), failedPrefixes.size(), failedObjects.size(), duration);

        return new ScanResult(modifiedObjects, overallSuccess, failedPrefixes, failedObjects);
    }

    /**
     * @Description 读取MinIO中的对象内容
     * @param bucketName 存储桶名称
     * @param objectName 对象名称
     * @return 文件内容字符串
     */
    public String readObjectAsString(String bucketName, String objectName) {
        try (InputStream stream = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build()
        )) {
            return IOUtils.toString(stream, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("读取对象内容失败: " + e.getMessage(), e);
        }
    }

    /**
     * 根据起始时间生成 yyyy/MM/dd/ 格式的路径前缀列表（包含当天到今天）
     */
    private List<String> generateDatePrefixes(Instant lastModifiedAfter, ZoneId zoneId) {
        LocalDate start = lastModifiedAfter.atZone(zoneId).toLocalDate();
        LocalDate end = LocalDate.now(zoneId);
        List<String> prefixes = new ArrayList<>();
        LocalDate current = start;

        while (!current.isAfter(end)){
            prefixes.add(String.format("%d/%02d/%02d/",
                    current.getYear(),
                    current.getMonthValue(),
                    current.getDayOfMonth()));
            current = current.plusDays(1);
        }
        return prefixes;
    }
}
