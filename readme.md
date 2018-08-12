# 完成elasticsearchtool使用说明
- （1）对应项目当中引入elasticsearchtool和elasticsearch相关组件的manven仓库坐标坐标
           选用版本：
            
       ```xml
          <elasticsearch.version>6.3.1</elasticsearch.version>
		   <log4j.version>2.7</log4j.version>
       ```
     maven仓库坐标：
            
       ```xml
       <dependency>
			<groupId>com.wd</groupId>
			<artifactId>elasticsearchtool</artifactId>
			<version>0.0.1-SNAPSHOT</version>
		</dependency>
		<dependency>
			<groupId>org.elasticsearch.client</groupId>
			<artifactId>transport</artifactId>
			<version>${elasticsearch.version}</version>
		</dependency>
		<dependency>
			<groupId>org.elasticsearch</groupId>
			<artifactId>elasticsearch</artifactId>
			<version>${elasticsearch.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.logging.log4j</groupId>
			<artifactId>log4j-core</artifactId>
			<version>${log4j.version}</version>
		</dependency>
       ```
- (2) application.yml文件进行配置

     ```java
     spring.data.elasticsearch.cluster-nodes: 192.168.199.139:9300
     spring.data.elasticsearch.cluster-name: elasticsearch-cluster
     ````    
- (3) 新建类ElasticSearchConfiguration的bean的注入

     ```java
     @Configuration
     public class ElasticSearchConfiguration {

				@Value("${spring.data.elasticsearch.cluster-nodes}")
				private String clusterNodes;
			
				@Value("${spring.data.elasticsearch.cluster-name}")
				private String clusterName;
			
				@Bean
				public ElasticSearchManage getElasticSearchManage() {
					return new ElasticSearchManageImpl();
				}
			
				@Bean(name = "client")
				public TransportClient client() throws UnknownHostException {
					ElasticSearchConfig elasticSearchConfig = new ElasticSearchConfig(clusterNodes,clusterName);
					TransportClient client = elasticSearchConfig.client();
					return client;
				}
			
			}
     ````
- (4) 相关业务实体创建，以下为对应demo，注意注解@PrimaryKey使用

     ```java
     @PrimaryKey("id")
     @Data
	  public class Book {
		
			private String id;
			private String author;
			private String title;
			private Integer word_count;
			private Date publish_date;
			private String gt_word_count;
			private String lt_word_count;
		
		
			
		}
     
     ```
- (5) service层继承ElasticSearchService<T>,可以自己实现对应的接口，在实现类当中指定对应的indexName和type名称,以下是对应demo：

   `public class AnalysisResultForESServiceImpl extends ElasticSearchService<AnalysisResultLogVO> implements AnalysisResultForESService`
    
    public final static String indexName = "analysisresultIndex";
    public final static String typeName = "analysisresultType";
   
   
            