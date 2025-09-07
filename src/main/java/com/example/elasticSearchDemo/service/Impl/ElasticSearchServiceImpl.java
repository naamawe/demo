package com.example.elasticSearchDemo.service.Impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.example.elasticSearchDemo.entity.FailedObjectInfo;
import com.example.elasticSearchDemo.entity.ScanResult;
import com.example.elasticSearchDemo.entity.Student;
import com.example.elasticSearchDemo.service.ElasticSearchService;
import com.example.elasticSearchDemo.util.ByteArrayMultipartFile;
import com.example.elasticSearchDemo.util.MinioUtils;
import com.example.elasticSearchDemo.util.SyncTimestampUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ElasticSearchServiceImpl implements ElasticSearchService {

    @Resource
    private ElasticsearchClient esClient;
    @Resource
    private MinioUtils minioUtils;

    String indexName = "student";

    @Override
    public String saveStudent(Student student) {
        try {
            CreateResponse createResponse = esClient.create(c -> c
                    .index(indexName)
                    .id(student.getId())
                    .document(student)
            );
            return createResponse.id();
        } catch (IOException e) {
            throw new RuntimeException("保存学生失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Student getStudentById(String id) {
        try {
            GetResponse<Student> getResp = esClient.get(g -> g
                            .index(indexName)
                            .id(id),
                    Student.class
            );
            return getResp.source();
        } catch (IOException e) {
            throw new RuntimeException("查询学生失败: " + e.getMessage(), e);

        }
    }

    @Override
    public List<Student> getAllStudents() {
        try {
            SearchResponse<Student> search = esClient.search(s -> s
                            .index(indexName)
                            .query(q -> q.matchAll(m -> m))
                    , Student.class
            );
            return search.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("查询学生失败: " + e.getMessage(), e);
        }
    }

    @Override
    public String deleteStudentById(String id) {
        try {
            DeleteResponse delete = esClient.delete(d -> d
                    .index(indexName)
                    .id(id)
            );
            return "删除成功，ID" + delete.id();
        } catch (IOException e) {
            throw new RuntimeException("删除学生失败: " + e.getMessage(), e);
        }
    }

    @Override
    public String updateStudent(Student student) {
        try {
            UpdateResponse<Student> update = esClient.update(u -> u
                            .index(indexName)
                            .id(student.getId())
                            .doc(student)
                    , Student.class
            );
            return update.id();
        } catch (IOException e) {
            throw new RuntimeException("更新学生失败: " + e.getMessage(), e);
        }
    }

    /**
     * 保存增量数据到minio
     * @param bucketName 桶名称
     * @return 结果
     */
    @Override
    public String getIncrementData(String bucketName) {
        Instant lastSyncTime = SyncTimestampUtil.readLastSyncTime(SyncTimestampUtil.SYNC_FILE_ES);
        log.info("ES开始增量同步, 上次同步时间: {}", lastSyncTime);
        //获取增量数据
        try {
            //使用游标分页查询避免OOM
            List<Hit<Student>> allHits = new ArrayList<>();
            List<FieldValue> searchAfter = null;
            do {
                SearchResponse<Student> response = executeEsQuery(lastSyncTime, searchAfter);
                List<Hit<Student>> hits = response.hits().hits();

                if (hits.isEmpty()) {
                    break;
                }
                allHits.addAll(hits);
                //获取最后一个文档的排序值作为下一页游标
                Hit<Student> lastHit = hits.get(hits.size() - 1);
                searchAfter = lastHit.sort();
            } while (true);
            //处理空查询结果
            if (allHits.isEmpty()) {
                log.info("未发现增量数据");
                return "未发现增量数据";
            }
            //获取最大更新时间
            Instant maxUpdateTime = allHits.stream()
                    .map(hit -> {
                        assert hit.source() != null;
                        return hit.source().getUpdateTime().toInstant();
                    })
                    .max(Instant::compareTo)
                    .orElse(lastSyncTime);
            //保存到MinIO
            String result = saveMinio(allHits);
            log.info("保存{}条数据到MinIO: {}", allHits.size(), result);
            //更新时间戳
            SyncTimestampUtil.saveLastSyncTime(SyncTimestampUtil.SYNC_FILE_ES, maxUpdateTime);
            log.info("更新ES同步时间戳: {}", maxUpdateTime);

            return String.format("成功保存%d条增量数据，下次同步时间: %s",
                    allHits.size(), maxUpdateTime);
        } catch (IOException e) {
            log.error("ES查询失败", e);
            return "数据查询异常";
        }catch (Exception e) {
            log.error("增量同步未知错误", e);
            return "系统内部错误";
        }
    }

    /**
     * 保存数据到ES
     * @param bucketName 桶名称
     * @return 结果
     */
    public String saveIncrementData(String bucketName) {
        Instant lastSyncTime = SyncTimestampUtil.readLastSyncTime(SyncTimestampUtil.SYNC_FILE_MINIO);
        log.info("MinIO 开始增量同步，上次同步时间: {}", lastSyncTime);

        // 调用扫描方法获取增量对象
        ScanResult scanResult = minioUtils.listObjectsModifiedAfter(bucketName, lastSyncTime);

        // 获取扫描结果
        Map<String, Instant> modifiedObjects = scanResult.getObjects();
        boolean scanOverallSuccess = scanResult.isSuccess();
        List<String> failedPrefixes = scanResult.getFailedPrefixes();
        List<FailedObjectInfo> failedObjects = scanResult.getFailedObjects();

        // 记录扫描失败的 prefix
        if (!failedPrefixes.isEmpty()) {
            log.error("严重：共 {} 个 prefix 扫描失败，可能导致数据遗漏！failedPrefixes={}",
                    failedPrefixes.size(), failedPrefixes);
            //TODO 存入失败桶
        }

        // 记录元数据解析失败的对象（个别文件问题）
        if (!failedObjects.isEmpty()) {
            log.warn("共 {} 个对象元数据解析失败，可能丢失部分文件", failedObjects.size());
            failedObjects.forEach(failure ->
                    log.debug("元数据解析失败详情: {}", failure.toString())
                    //TODO 存入失败桶
            );
        }

        // 如果没有任何对象被成功扫描到，且整体扫描也不成功，应视为异常
        if (modifiedObjects.isEmpty()) {
            if (!scanOverallSuccess) {
                log.error("扫描未成功完成，且无任何有效对象返回，可能存在严重问题。failedPrefixes={}, failedObjects={}",
                        failedPrefixes.size(), failedObjects.size());
                return "扫描失败：无数据返回且存在扫描错误，需排查";
            } else {
                log.info("未找到修改时间大于 {} 的文件", lastSyncTime);
                return "无增量数据";
            }
        }

        // 处理成功扫描到的对象
        int successCount = 0;
        int failureCount = 0;

        for (Map.Entry<String, Instant> entry : modifiedObjects.entrySet()) {
            String objectName = entry.getKey();
            try {
                String fileContent = minioUtils.readObjectAsString(bucketName, objectName);
                Student student = parseStudentFromFile(objectName, fileContent);
                saveStudent(student);
                successCount++;
                log.debug("成功保存学生数据: {} - {}", student.getId(), objectName);
            } catch (Exception e) {
                failureCount++;
                log.error("处理文件失败: {} - {}", objectName, e.getMessage(), e);
            }
        }

        // 计算最新的最后修改时间
        Instant latestModifiedTime = modifiedObjects.values().stream()
                .max(Instant::compareTo)
                .orElse(lastSyncTime);

        // 更新同步时间戳
        SyncTimestampUtil.saveLastSyncTime(SyncTimestampUtil.SYNC_FILE_ES, latestModifiedTime);
        log.info("更新 MinIO 同步时间戳: {}", latestModifiedTime);

        // 构造最终返回信息，包含全面的状态摘要
        String resultMessage = String.format(
                "增量同步完成：成功保存%d条，失败%d条。" +
                        "扫描状态=%s，failedPrefixes=%d，failedObjects=%d，下次同步时间=%s",
                successCount, failureCount,
                scanOverallSuccess ? "成功" : "部分失败",
                failedPrefixes.size(),
                failedObjects.size(),
                latestModifiedTime
        );

        log.info(resultMessage);
        return resultMessage;
    }


    /**
     * 解析json
     * @param objectName 文件原始名
     * @param fileContent 类型
     * @return 学生对象
     */
    private Student parseStudentFromFile(String objectName, String fileContent) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            // 文件内容就是一个 JSON 对象，直接反序列化
            return mapper.readValue(fileContent, Student.class);
        } catch (Exception e) {
            log.error("文件解析失败 - 文件名: {} - 内容: {}", objectName, fileContent);
            throw new RuntimeException("解析学生数据失败: " + objectName, e);
        }
    }


    /**
     * 封装ES查询逻辑
     * @param lastSyncTime 时间戳
     * @param searchAfter 游标分页
     * @return 查询结果
     * @throws IOException 抛出异常
     */
    private SearchResponse<Student> executeEsQuery(Instant lastSyncTime, List<FieldValue> searchAfter) throws IOException {

        String formattedTime = DateTimeFormatter.ISO_INSTANT.format(lastSyncTime);

        SearchRequest.Builder builder = new SearchRequest.Builder()
                .index(indexName)
                .size(1000)
                .query(q -> q
                        .range(r -> r
                                .field("updateTime")
                                .gt(JsonData.of(formattedTime))
                        )
                )
                .sort(so -> so
                        .field(f -> f
                                .field("updateTime")
                                .order(SortOrder.Asc)
                        )
                )
                .sort(so -> so
                        .field(f -> f
                                .field("id.keyword")
                                .order(SortOrder.Asc)
                        )
                );

        // 添加游标分页
        if (searchAfter != null && !searchAfter.isEmpty()) {
            builder.searchAfter(searchAfter);
        }
        return esClient.search(builder.build(), Student.class);
    }

    /**
     * 保存数据到minio
     * @param hits 原始数据
     * @return 信息
     */
    private String saveMinio(List<Hit<Student>> hits) {
        try {
            for (Hit<Student> hit : hits) {
                Student student = hit.source();
                assert student != null;
                ObjectMapper mapper = new ObjectMapper();
                mapper.registerModule(new JavaTimeModule());

                // 转换为 JSON
                String json = mapper.writeValueAsString(student);

                MultipartFile multipartFile = getMultipartFile(json);
                minioUtils.upload(multipartFile, "test");
            }
            return "保存成功，共 " + hits.size() + " 个学生";
        } catch (Exception e) {
            log.error("保存到MinIO失败", e);
            return null;
        }
    }


    /**
     * 转换文件
     * @param jsonData 原始数据
     * @return 转换文件
     */
    private static MultipartFile getMultipartFile(String jsonData) {
        byte[] content = jsonData.getBytes(StandardCharsets.UTF_8);
        //创建MockMultipartFile
        return new ByteArrayMultipartFile(
                "increment_data",
                "increment_data.json",
                "application/json",
                content
        );
    }


}
