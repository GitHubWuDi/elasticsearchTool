package com.example.elasticsearch.config;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
* @author wudi
* @version 创建时间：2018年7月22日 下午4:46:55
* @ClassName ElasticSearchConfig
* @Description es连接配置
*/
@Configuration
public class ElasticSearchConfig {
   
	@Bean
	public TransportClient client() throws UnknownHostException{
		TransportAddress node  = new TransportAddress(InetAddress.getByName("192.168.199.139"), 9300);
		Settings settings = Settings.builder().put("cluster.name","elasticsearch-cluster").build();
		TransportClient client = new PreBuiltTransportClient(settings);
		client.addTransportAddress(node);
		return client;
	}
}
