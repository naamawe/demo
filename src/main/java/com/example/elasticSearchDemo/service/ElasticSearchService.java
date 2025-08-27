package com.example.elasticSearchDemo.service;

import com.example.elasticSearchDemo.pojo.Student;

import java.util.List;

public interface ElasticSearchService {

    String saveStudent(Student student);

    Student getStudentById(String id);

    List<Student> getAllStudents();

    String deleteStudentById(String id);

    String updateStudent(Student student);

    String getIncrementData(String bucketName);

}
