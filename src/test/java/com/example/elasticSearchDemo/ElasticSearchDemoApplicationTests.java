package com.example.elasticSearchDemo;

import com.example.elasticSearchDemo.pojo.Student;
import com.example.elasticSearchDemo.service.Impl.ElasticSearchServiceImpl;
import com.example.elasticSearchDemo.util.MinioUtils;
import com.example.elasticSearchDemo.util.SyncTimestampManager;
import io.minio.messages.Bucket;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

@SpringBootTest
class ElasticSearchDemoApplicationTests {

	@Autowired
	private MinioUtils minioUtils;
	@Autowired
	private ElasticSearchServiceImpl elasticSearchService;

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
		SyncTimestampManager.saveLastSyncTime(SyncTimestampManager.SYNC_FILE_MINIO, Instant.now());
	}

	@Test
	void readTimeTest() {
		Instant instant = SyncTimestampManager.readLastSyncTime(SyncTimestampManager.SYNC_FILE_ES);
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
		Student student = new Student();
		student.setId("4");
		student.setName("王六");
		student.setAge(20);
		student.setSex("男");
		student.setCreateTime(Timestamp.valueOf(LocalDateTime.now()));
		student.setUpdateTime(Timestamp.valueOf(LocalDateTime.now()));
		String s = elasticSearchService.saveStudent(student);
		System.out.println(s);
	}

	//从Minio获取增量数据
	@Test
	void saveIncrementData() {
		String msg = elasticSearchService.saveIncrementData("test");
		System.out.println(msg);
	}

}
