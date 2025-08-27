package com.example.elasticSearchDemo.scheduler;

import com.example.elasticSearchDemo.service.Impl.ElasticSearchServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;

/**
 * @author Echo009
 * @since 2022/4/27
 */
@Component
public class SaveDataFromMinioProcessor implements BasicProcessor {
    @Autowired
    private ElasticSearchServiceImpl elasticSearchService;

    @Override
    public ProcessResult process(TaskContext context) {
        try {
            // 1. 参数解析与验证
            String jobParams = context.getJobParams();
            if (jobParams == null || jobParams.trim().isEmpty()) {
                return new ProcessResult(false, "任务参数不能为空");
            }

            // 2. 提取 bucketName
            String bucketName = jobParams.startsWith("bucket=")
                    ? jobParams.substring(7).trim()
                    : jobParams.trim();

            if (bucketName.isEmpty()) {
                return new ProcessResult(false, "bucket名称不能为空");
            }

            // 3. 执行核心任务
            String result = elasticSearchService.saveIncrementData(bucketName);

            // 4. 结果处理
            return new ProcessResult(
                    !result.contains("失败") && !result.contains("异常"),
                    result
            );
        } catch (Exception e) {
            return new ProcessResult(false, "任务执行异常: " + e.getMessage());
        }
    }
}