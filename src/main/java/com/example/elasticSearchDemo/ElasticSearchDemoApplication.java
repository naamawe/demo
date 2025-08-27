package com.example.elasticSearchDemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableConfigurationProperties
@SpringBootApplication
public class ElasticSearchDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(ElasticSearchDemoApplication.class, args);
	}

}
