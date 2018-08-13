package com.example.elasticsearch.config;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
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
   
	@Value("${spring.data.elasticsearch.cluster-nodes}")
	private String clusterNodes;

	@Value("${spring.data.elasticsearch.cluster-name}")
	private String clusterName;
	
	//TODO 加入对应的项目中需要添加
//	public ElasticSearchConfig(String clusterNodes,String clusterName){
//		this.clusterNodes = clusterNodes;
//		this.clusterName = clusterName;
//	}
	
	@Bean
	public TransportClient client() throws UnknownHostException{
		Settings settings = Settings.builder().put("cluster.name",clusterName).build();
		TransportClient client = new PreBuiltTransportClient(settings);
		String[] splitNodes = clusterNodes.split(",");
		for (String node : splitNodes) {
			String ip = node.split(":")[0];
			String port = node.split(":")[1];
			TransportAddress transNode  = new TransportAddress(InetAddress.getByName(ip), Integer.valueOf(port));
			client.addTransportAddress(transNode);
		}
		return client;
	
	}
}
