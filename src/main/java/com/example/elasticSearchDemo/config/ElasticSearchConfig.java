package com.example.elasticSearchDemo.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.example.elasticSearchDemo.util.CustomJsonpMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfig {  
  
    @Bean
    public ElasticsearchClient esClient() {
        // ES服务器URL  
        String serverUrl = "http://127.0.0.1:9200";  
        // ES用户名和密码
//        String userName = "elastic";
//        String password = "Z+pZSFvDK*BB1jD9fx+g";

//        BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
//        credsProv.setCredentials(
//                AuthScope.ANY, new UsernamePasswordCredentials(userName, password)
//        );
  
        RestClient restClient = RestClient
                .builder(HttpHost.create(serverUrl))
//                .setHttpClientConfigCallback(hc -> hc.setDefaultCredentialsProvider(credsProv))
                .build();  
  
        ElasticsearchTransport transport = new RestClientTransport(
                restClient,
                CustomJsonpMapper.create()
        );
        ElasticsearchClient esClient = new ElasticsearchClient(transport);
        return esClient;
    }
  
}