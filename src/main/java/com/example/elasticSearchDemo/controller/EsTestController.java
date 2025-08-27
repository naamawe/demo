package com.example.elasticSearchDemo.controller;

import com.example.elasticSearchDemo.pojo.Student;
import com.example.elasticSearchDemo.service.ElasticSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class EsTestController {
    @Autowired
    private ElasticSearchService es;

    /**
     * 新增学生
     * @param student 学生
     * @return        返回值
     */
    @PostMapping("save")
    public String saveStudent(@RequestBody Student student) {
        return es.saveStudent(student);
    }

    /**
     * 根据ID查询学生
     * @param id id
     * @return   返回值
     */
    @GetMapping("/{id}")
    public Student getStudentById(@PathVariable String id) {
        return es.getStudentById(id);
    }

    /**
     * 查询所有学生
     * @return 返回值
     */
    @GetMapping("getAll")
    public List<Student> getAllStudents() {
        return es.getAllStudents();
    }

    /**
     * 删除学生
     * @param id id
     * @return   返回值
     */
    @DeleteMapping("/{id}")
    public String deleteStudent(@PathVariable String id) {
        return es.deleteStudentById(id);
    }

    /**
     * 更新学生信息
     * @param student 学生
     * @return        返回值
     */
    @PostMapping("update")
    public String updateStudent(@RequestBody Student student){
        return es.updateStudent(student);
    }

}
