package com.example.elasticSearchDemo.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {
    private String id;
    private String name;
    private Integer age;
    private String sex;
    private Timestamp updateTime;
    private Timestamp createTime;
}
