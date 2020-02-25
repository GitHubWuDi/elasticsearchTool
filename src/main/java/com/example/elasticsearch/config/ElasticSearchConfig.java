package com.example.elasticsearch.config;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;

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

	@Value("${spring.data.elasticsearch.ssl_path}")
	private String ssl_path;
	
	@Value("${spring.data.elasticsearch.transport.enabled}")
	private Boolean transport_enabled;
	@Value("${spring.data.elasticsearch.transport.keystore_filepath}")
	private String transport_keystore_filepath;
	@Value("${spring.data.elasticsearch.transport.truststore_filepath}")
	private String transport_truststore_filepath;
	@Value("${spring.data.elasticsearch.transport.keystore_password}")
	private String transport_keystore_password;
	@Value("${spring.data.elasticsearch.transport.truststore_password}")
	private String transport_truststore_password;
	
	
	
	@Value("${spring.data.elasticsearch.http.enabled}")
	private Boolean http_enabled;
	@Value("${spring.data.elasticsearch.http.keystore_filepath}")
	private String http_keystore_filepath;
	@Value("${spring.data.elasticsearch.http.truststore_filepath}")
	private String http_truststore_filepath;
	@Value("${spring.data.elasticsearch.http.keystore_password}")
	private String http_keystore_password;
	@Value("${spring.data.elasticsearch.http.truststore_password}")
	private String http_truststore_password;
	
	public static   final  String  es_ssl_path="/usr/share/elasticsearch/config/";
	
	@Bean
	public TransportClient client() throws UnknownHostException{
		
	 
	
		/*Settings settings = Settings.builder()
				.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, basePath+"truststore.jks")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, basePath+"admin-keystore.jks")
                //.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, basePath+"root-ca.pem")
				//.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD, "vrv@12345")
				.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, "vrv@12345")
				.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, "vrv@12345")
				.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
				.put("client.transport.sniff",false)
				.put("cluster.name",clusterName)
				.put("searchguard.ssl.transport.enabled",true)
				.build();*/
		
		if(StringUtils.isEmpty(ssl_path)) {
			ssl_path=es_ssl_path;
		}
		File file=new File(ssl_path);
		if(!file.exists()||file.isFile()) {
			ssl_path=es_ssl_path;
		}
		
		if(!ssl_path.endsWith("/")) {
			ssl_path=ssl_path+"/";
		}
		
		
		if(StringUtils.isEmpty(http_keystore_filepath)) {
			http_keystore_filepath="admin-keystore.jks";
		}
		if(StringUtils.isEmpty(transport_keystore_filepath)) {
			transport_keystore_filepath="admin-keystore.jks";
		}
		
		if(StringUtils.isEmpty(http_truststore_filepath)) {
			http_truststore_filepath="truststore.jks";
		}
		if(StringUtils.isEmpty(transport_truststore_filepath)) {
			transport_truststore_filepath="truststore.jks";
		}
		
		Settings settings = Settings.builder()
				.put("cluster.name", clusterName)
				.put("searchguard.ssl.transport.enabled", transport_enabled)
			    .put("searchguard.ssl.transport.keystore_filepath", ssl_path+transport_keystore_filepath)
			    .put("searchguard.ssl.transport.truststore_filepath", ssl_path+transport_truststore_filepath)
				.put("searchguard.ssl.transport.keystore_password", transport_keystore_password)
				.put("searchguard.ssl.transport.truststore_password", transport_truststore_password)
				.put("searchguard.ssl.transport.enforce_hostname_verification", false)
				
				
				.put("searchguard.ssl.http.enabled", http_enabled)
			    .put("searchguard.ssl.http.keystore_filepath", ssl_path+http_keystore_filepath)
			    .put("searchguard.ssl.http.truststore_filepath", ssl_path+http_truststore_filepath)
				.put("searchguard.ssl.http.keystore_password", http_keystore_password)
				.put("searchguard.ssl.http.truststore_password", http_truststore_password)

				.build();
 
		
		TransportClient client = new PreBuiltTransportClient(settings,SearchGuardSSLPlugin.class);
		//TransportClient client = new PreBuiltTransportClient(settings);
		String[] splitNodes = clusterNodes.split(",");
		for (String node : splitNodes) {
			String ip = node.split(":")[0];
			String port = node.split(":")[1];
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), Integer.parseInt(port)));
//			TransportAddress transNode  = new TransportAddress(InetAddress.getByName(ip), Integer.valueOf(port));
//			client.addTransportAddress(transNode);
		}
		return client;
	
	}
	
	 
	
}
