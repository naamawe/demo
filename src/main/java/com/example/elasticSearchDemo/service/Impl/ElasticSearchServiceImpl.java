package com.example.elasticSearchDemo.service.Impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.example.elasticSearchDemo.pojo.Student;
import com.example.elasticSearchDemo.service.ElasticSearchService;
import com.example.elasticSearchDemo.util.ByteArrayMultipartFile;
import com.example.elasticSearchDemo.util.MinioUtils;
import com.example.elasticSearchDemo.util.SyncTimestampManager;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ElasticSearchServiceImpl implements ElasticSearchService {

    @Autowired
    private ElasticsearchClient esClient;
    @Autowired
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
        Instant lastSyncTime = SyncTimestampManager.readLastSyncTime(SyncTimestampManager.SYNC_FILE_ES);
        log.info("ES开始增量同步, 上次同步时间: {}", lastSyncTime);

        try {
            //使用游标分页查询避免OOM
            List<Hit<Student>> allHits = new ArrayList<>();
            List<FieldValue> searchAfter = null;
            do {
                SearchResponse<Student> response = executeEsQuery(lastSyncTime, searchAfter);
                List<Hit<Student>> hits = response.hits().hits();

                if (hits.isEmpty()) break;

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
            SyncTimestampManager.saveLastSyncTime(SyncTimestampManager.SYNC_FILE_ES, maxUpdateTime);
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
        Instant lastSyncTime = SyncTimestampManager.readLastSyncTime(SyncTimestampManager.SYNC_FILE_MINIO);
        log.info("Minio开始增量同步, 上次同步时间: {}", lastSyncTime);

        //获取增量数据
        Map<String, Instant> modifiedObjects = minioUtils.listObjectsModifiedAfter(bucketName, lastSyncTime);
        if (modifiedObjects.isEmpty()){
            log.info("未找到修改时间大于 {} 的文件", lastSyncTime);
            return "无增量数据";
        }
        Instant latestModifiedTime = modifiedObjects.values().stream()
                .max(Instant::compareTo)
                .orElse(lastSyncTime);

        int successCount = 0;
        int failureCount = 0;
        //保存到ES
        for (Map.Entry<String, Instant> entry : modifiedObjects.entrySet()) {
            String objectName = entry.getKey();
            try {
                //从MinIO获取文件内容
                String fileContent = minioUtils.readObjectAsString(bucketName, objectName);

                //解析文件内容
                Student student = parseStudentFromFile(objectName, fileContent);

                //保存到ES
                saveStudent(student);
                successCount++;

                log.debug("成功保存学生数据: {} - {}", student.getId(), objectName);
            } catch (Exception e) {
                failureCount++;
                log.error("处理文件失败: {} - {}", objectName, e.getMessage(), e);
            }
        }
        //更新时间戳
        SyncTimestampManager.saveLastSyncTime(SyncTimestampManager.SYNC_FILE_ES, latestModifiedTime);
        log.info("更新Minio同步时间戳: {}", latestModifiedTime);

        return String.format("成功保存%d条增量数据，保存失败%d条增量数据，下次同步时间: %s",
                successCount, failureCount, latestModifiedTime);
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
            mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            JsonNode root = mapper.readTree(fileContent);

            if (root.isArray()) {
                if (root.isEmpty()) {
                    throw new RuntimeException("JSON数组为空，无法解析为学生对象");
                }
                // 取数组中的第一个元素
                JsonNode firstElement = root.get(0);
                return mapper.treeToValue(firstElement, Student.class);
            }else if (root.isObject()) {
                //解析整个对象
                return mapper.treeToValue(root, Student.class);
            }
            // 处理无效JSON格式
            else {
                throw new RuntimeException("无效的JSON格式，必须是对象或数组");
            }
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

        String formattedTime = DateTimeFormatter.ISO_INSTANT.format(
                Instant.ofEpochMilli(lastSyncTime.toEpochMilli())
        );
        RangeQuery rangeQuery = RangeQuery.of(r -> r
                .field("updateTime")
                .gt(JsonData.of(formattedTime))
        );

        SearchRequest.Builder builder = new SearchRequest.Builder()
                .index(indexName)
                .size(1000) // 分页大小
                .query(q -> q
                        .range(rangeQuery)
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
                )
                .source(src -> src
                        .filter(f -> f
                                .includes("id", "name", "age", "sex", "updateTime")
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
            // 1. 将数据转换为JSON字符串
            StringBuilder jsonBuilder = new StringBuilder("[");
            for (int i = 0; i < hits.size(); i++) {
                Student student = hits.get(i).source();
                jsonBuilder.append("{");
                assert student != null;
                jsonBuilder.append("\"id\":\"").append(student.getId()).append("\",");
                jsonBuilder.append("\"name\":\"").append(student.getName()).append("\",");
                jsonBuilder.append("\"age\":").append(student.getAge()).append(",");
                jsonBuilder.append("\"sex\":\"").append(student.getSex()).append("\",");
                jsonBuilder.append("\"updateTime\":\"").append(student.getUpdateTime()).append("\"");
                jsonBuilder.append("}");

                if (i < hits.size() - 1) {
                    jsonBuilder.append(",");
                }
            }
            jsonBuilder.append("]");

            MultipartFile multipartFile = getMultipartFile(jsonBuilder);

            // 4. 调用MinioUtils上传
            return minioUtils.upload(multipartFile, "test");

        } catch (Exception e) {
            System.err.println("保存到MinIO失败: " + e.getMessage());
            return null;
        }
    }

    /**
     * 转换文件
     * @param jsonBuilder 原始数据
     * @return 转换文件
     */
    private static MultipartFile getMultipartFile(StringBuilder jsonBuilder) {
        String jsonData = jsonBuilder.toString();
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
