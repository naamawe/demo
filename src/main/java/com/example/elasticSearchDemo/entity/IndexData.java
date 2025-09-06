package com.example.elasticSearchDemo.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IndexData {

    /**
     * 主键
     */
    private Long id;

    private String name;

    private Integer age;
}
