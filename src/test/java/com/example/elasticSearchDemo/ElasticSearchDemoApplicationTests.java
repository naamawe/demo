package com.example.elasticSearchDemo;

import com.example.elasticSearchDemo.entity.IndexData;
import com.example.elasticSearchDemo.entity.Student;
import com.example.elasticSearchDemo.service.Impl.ElasticSearchServiceImpl;
import com.example.elasticSearchDemo.strategy.FileParseStrategy;
import com.example.elasticSearchDemo.strategy.factory.FileParseStrategyFactory;
import com.example.elasticSearchDemo.util.IOUtils;
import com.example.elasticSearchDemo.util.MinioUtils;
import com.example.elasticSearchDemo.util.SyncTimestampUtil;
import io.minio.messages.Bucket;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class ElasticSearchDemoApplicationTests {

	@Autowired
	private MinioUtils minioUtils;
	@Autowired
	private ElasticSearchServiceImpl elasticSearchService;
	@Autowired
	private IOUtils ioUtils;
	@Autowired
	private FileParseStrategyFactory strategyFactory;

	/**
	 * 判断bucket是否存在
	 */
	@Test
	void existBucketTest() throws Exception{
		Boolean existBucket = minioUtils.existBucket("test1");
		System.out.println(existBucket);
	}

	/**
	 * 创建桶
	 */
	@Test
	void makeBucketTest() {
		Boolean makeBucket = minioUtils.makeBucket("test2");
		System.out.println(makeBucket);
	}

	/**
	 * 删除桶
	 */
	@Test
	void removeBucketTest() {
		Boolean removeBucket = minioUtils.removeBucket("test2");
		System.out.println(removeBucket);
	}

	/**
	 * 桶列表
	 * @throws Exception 抛出异常
	 */
	@Test
	void listBucketsTest() throws Exception {
		List<Bucket> buckets = minioUtils.listBuckets();
		for (Bucket bucket : buckets) {
			System.out.println(bucket.name());
		}
	}

	/**
	 * @Description 上传文件
	 * @throws Exception 抛出异常
	 */
	@Test
	void uploadTest() throws Exception{
		byte[] fileContent = "测试文件内容".getBytes();
		MockMultipartFile file = new MockMultipartFile(
				"file",
				"test.txt",
				"text/plain",
				fileContent
		);
		String name = minioUtils.upload(file, "test");
		System.out.println(name);
	}

	/**
	 * 删除文件
	 * @throws Exception 抛出异常
	 */
	@Test
	void removeFileTest() throws Exception{
		minioUtils.removeFile("test", "005e8f408b3a46e083b69a3e29f0d6d3.txt");
	}

	/**
	 * 获取文件url
	 * @throws Exception 抛出异常
	 */
	@Test
	void getFileUrl() throws Exception{
		String url = minioUtils.getFileUrl("test", "005e8f408b3a46e083b69a3e29f0d6d3.txt");
		System.out.println(url);
	}

	@Test
	void saveTimeTest() {
//		SyncTimestampManager.saveLastSyncTime(SyncTimestampManager.SYNC_FILE_ES, Instant.now());
		SyncTimestampUtil.saveLastSyncTime(SyncTimestampUtil.SYNC_FILE_MINIO, Instant.now());
	}

	@Test
	void readTimeTest() {
		Instant instant = SyncTimestampUtil.readLastSyncTime(SyncTimestampUtil.SYNC_FILE_ES);
		System.out.println(instant);
	}

	//从ES获取增量数据
	@Test
	void getIncrementDataTest() {
		String msg = elasticSearchService.getIncrementData("test");
		System.out.println(msg);
	}

	//获取学生
	@Test
	void getStudentByIdTest() {
		Student student = elasticSearchService.getStudentById("1");
		System.out.println(student);
	}

	//删除学生
	@Test
	void deleteStudentTest() {
		String s = elasticSearchService.deleteStudentById("4");
		System.out.println(s);
	}

	//保存学生
	@Test
	void saveStudentTest() {
		// 准备测试数据
		Student student = new Student();
		student.setId("test-student-001");
		student.setName("张三");
		student.setAge(20);
		student.setSex("男");
		
		java.util.Date currentDate = new java.util.Date();
		student.setCreateTime(new java.sql.Date(currentDate.getTime()));
		student.setUpdateTime(new java.sql.Date(currentDate.getTime()));
		
		// 执行保存操作
		String result = elasticSearchService.saveStudent(student);
		
		// 验证操作结果
		assertNotNull(result);
		assertFalse(result.trim().isEmpty());
		
		// 验证数据确实被保存
		Student savedStudent = elasticSearchService.getStudentById(student.getId());
		assertNotNull(savedStudent);
		assertEquals("张三", savedStudent.getName());
		assertEquals(20, savedStudent.getAge());
		
		// 清理测试数据
		elasticSearchService.deleteStudentById(student.getId());
	}

	//从Minio获取增量数据
	@Test
	void saveIncrementData() {
		String msg = elasticSearchService.saveIncrementData("test");
		System.out.println(msg);
	}

	@Test
	void testIO() {
		File[] files = ioUtils.fullGetFileFromPath("src/main/resources/mydocument",
				"explorer_objexp_biz_monitor_index_data",
				"20250905");
		for (File file : files) {
			System.out.println(file.getName());
		}
	}

	@Test
	void testIndexDataParse() throws IOException {

		File tempFile = File.createTempFile("index_data_test", ".txt");
		try (FileWriter writer = new FileWriter(tempFile)) {
			writer.write("id,name,age\n"); // 表头
			writer.write("1,aaa,12\n");
			writer.write("2,bbb,34\n");
			writer.write("3,ccc,88\n");
		}

		FileParseStrategy<IndexData> strategy  = strategyFactory.getStrategy("index_data");
		List<IndexData> result = IOUtils.parseFiles(new File[]{tempFile}, strategy);

		assertEquals(3, result.size());
		assertEquals("aaa", result.get(0).getName());
		assertEquals(34, result.get(1).getAge());
		assertEquals(3L, result.get(2).getId());
	}
}
