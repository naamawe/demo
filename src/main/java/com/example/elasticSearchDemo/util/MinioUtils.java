package com.example.elasticSearchDemo.util;

import com.example.elasticSearchDemo.config.MinioConfig;
import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;


@Component
public class MinioUtils {

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
        assert fileName != null;
        String objectName = UUID.randomUUID().toString().replaceAll("-", "")
                + fileName.substring(fileName.lastIndexOf("."));

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
     * @Description 获取最后修改时间大于指定时间的对象列表
     * @param bucketName 存储桶名称
     * @param lastModifiedAfter 最后修改时间的起始点
     * @return 符合条件的对象列表
     */
    public Map<String, Instant> listObjectsModifiedAfter(String bucketName, Instant lastModifiedAfter) {
        Map<String, Instant> modifiedObjects = new HashMap<>();
        try {
            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(bucketName)
                            .recursive(true)
                            .build()
            );

            for (Result<Item> result : results) {
                Item item = result.get();
                ZonedDateTime lastModified = item.lastModified();
                Instant modifiedInstant = lastModified.toInstant();

                // 比较最后修改时间
                if (modifiedInstant.isAfter(lastModifiedAfter)) {
                    modifiedObjects.put(item.objectName(), modifiedInstant);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("获取修改对象列表失败: " + e.getMessage(), e);
        }
        return modifiedObjects;
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
}
