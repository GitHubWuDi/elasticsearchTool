package com.example.elasticsearch;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.elasticsearch.service.ElasticSearchManage;
import com.example.elasticsearch.vo.BookVO;

/**
 * @author wudi
 * @version 创建时间：2018年7月28日 下午3:51:46
 * @ClassName ElasticsearchManageTests
 * @Description ES底层接口实现类单元测试
 */

@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticsearchManageTests {

	private static Logger logger = Logger.getLogger(ElasticsearchManageTests.class);
	public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

	@Autowired
	private ElasticSearchManage elasticSearchManage;

	/**
	 * 索引是否存在
	 */
	@Test
	public void isExistEsIndexTest() {
		String index = "book";
		Boolean result = elasticSearchManage.isExistEsIndex(index);
		assertEquals(true, result);
	}

	/**
	 * 通过indexName创建索引
	 */
	@Test
	public void createSimpleIndexTest() {
		String indexName = "test";
		Boolean result = elasticSearchManage.createEsIndex(indexName);
		assertEquals(true, result);
	}

	/**
	 * 创建索引-属性创建索引测试（包含index，shardCount，repliceCount）
	 */
	@Test
	public void createIndexByShardAndRepliceTest() {
		String indexName = "test";
		int shardCount = 5;
		int repliceCount = 1;
		Boolean result = elasticSearchManage.createEsIndex(indexName, shardCount, repliceCount);
		assertEquals(false, result);
	}

	/**
	 * 创建索引-属性创建索引（包含index，mapping，shardCount，repliceCount，fileds）
	 */
	@Test
	public void createIndexByMappingTest() {
		String indexName = "books";
		String mapping = "test";
		int shardCount = 5;
		int repliceCount = 1;
		BookVO bookVO = new BookVO();
		Field[] fields = bookVO.getClass().getDeclaredFields();
		Boolean result = elasticSearchManage.createEsIndex(indexName, mapping, shardCount, repliceCount, fields);
		assertEquals(true, result);
	}

	/**
	 * 刷新所有索引测试
	 */
	@Test
	public void refreshAllIndexTest() {
		elasticSearchManage.refreshAllIndex();
		assertEquals(true, true);
	}

	/**
	 * 刷新指定索引测试
	 */
	@Test
	public void refreshIndexByIndexNameTest() {
		String indexName = "books";
		elasticSearchManage.refreshIndexByIndexName(indexName);
		assertEquals(true, true);
	}

	/**
	 * 测试获得mapping的信息
	 */
	@Test
	public void getMappingTest() {
		String indexName = "books";
		String type = "test";
		Map<String, Object> map = elasticSearchManage.getMapping(indexName, type);
		assertEquals(true, true);
	}

	/**
	 * 重新设置副本的个数
	 */
	@Test
	public void setRepliceCountNumTest() {
		String indexName = "books";
		int repliceCount = 6;
		boolean result = elasticSearchManage.setRepliceCountNum(indexName, repliceCount);
		assertEquals(true, result);
	}

	/**
	 * 索引文档库总数测试
	 */
	@Test
	public void getDocCountByIndexNameTest() {
		String indexName = "book";
		long docCountByIndexName = elasticSearchManage.getDocCountByIndexName(indexName);
		assertEquals(6, docCountByIndexName);
	}

	/**
	 * 根据索引名称获得总的分片数
	 */
	@Test
	public void getShardCountByIndexNameTest() {
		String indexName = "book";
		int shardCountByIndexName = elasticSearchManage.getShardCountByIndexName(indexName);
		assertEquals(5, shardCountByIndexName);
	}

	/**
	 * 获得索引成功启动的分片总数
	 */
	@Test
	public void getSuccessShardCountTest() {
		String indexName = "book";
		int shardCountByIndexName = elasticSearchManage.getSuccessShardCount(indexName);
		assertEquals(5, shardCountByIndexName);
	}

	/**
	 * 获得副本数
	 */
	@Test
	public void getAllRepliceCountTest() {
		String indexName = "book";
		int shardCountByIndexName = elasticSearchManage.getAllRepliceCount(indexName);
		assertEquals(1, shardCountByIndexName);
	}

	/**
	 * 获得Setting的内容
	 */
	@Test
	public void getSettingTest() {
		String indexName = "kafka_elk_newlog-2018.07.23";
		String setting = elasticSearchManage.getSetting(indexName);
	}

	/**
	 * 创建对应的文档修改
	 */
	@Test
	public void createDocTest() {
		String format = format(new Date());
		String indexName = "books";
		String type = "test";
		String id = "3";
		Map<String, Object> map = new HashMap<>();
		map.put("title", "Elasticsearch 入门");
		map.put("author", "wudi");
		map.put("word_count", 3000);
		map.put("publish_date", format);
		map.put("gt_word_count", 100);
		map.put("lt_word_count", 2000);
		String result = elasticSearchManage.createDoc(indexName, type, id, map);
		assertEquals("success", result);
	}

	public static String format(Date date) {
		SimpleDateFormat formatTool = new SimpleDateFormat();
		formatTool.applyPattern(DEFAULT_DATE_PATTERN);
		return formatTool.format(date);
	}

	/**
	 * 删除对应Doc的Test
	 */
	@Test
	public void delDocTest() {
		String indexName = "books";
		String type = "test";
		String id = "1";
		Boolean delDocByIndexName = elasticSearchManage.delDocByIndexName(indexName, type, id);
		assertEquals(delDocByIndexName, true);
	}

	/**
	 * 根据索引名称删除索引
	 */
	@Test
	public void delIndexTest() {
		String indexName = "packetbeat-6.3.1-2018.07.16";
		Boolean delDocByIndexName = elasticSearchManage.delIndexByIndexName(indexName);
		assertEquals(delDocByIndexName, true);
	}

	/**
	 * 获得所有的索引
	 */
	@Test
	public void getAllIndex() {
		String[] allIndex = elasticSearchManage.getAllIndex();
		assertEquals(12, allIndex.length);
	}

	/**
	 * 获得所有的索引的个数
	 */
	@Test
	public void getAllIndexCount() {
		int count = elasticSearchManage.getAllIndexCount();
		assertEquals(12, count);
	}
   
	
	/**
	 * 根据索引获得对应的type集合
	 */
	@Test
	public void getTypeByIndexTest() {
		String indexName = "books";
		List<String> list = elasticSearchManage.getTypesByIndexName(indexName);
		assertEquals(1, list.size());
	}
	
	/**
	 * 获得index下面所有的fields
	 */
	@Test
	public void getAllFieldsByIndexNameTest() {
		String indexName = "books";
		Set<String> set = elasticSearchManage.getAllFieldsByIndexName(indexName);
		assertEquals(6, set.size());
	}
	
	/**
	 * 根据Index和Type获得对应成员变量
	 */
	@Test
	public void getAllFieldsByIndexNameAndTypeTest() {
		String indexName = "books";
		String type = "test";
		Set<String> set = elasticSearchManage.getAllFieldsByIndexNameAndType(indexName, type);
		assertEquals(6, set.size());
	}
	
	/**
	 * 根据index,type,id获得doc信息
	 */
   @Test
   public void getDocTest() {
		String indexName = "books";
		String type = "test";
		String id ="1";
		Map<String, Object> map = elasticSearchManage.getDoc(indexName, type, id);
		logger.info("map info:" + map);
	}
	
   /**
    * 获得es集群名称
    */
   @Test
   public void getClusterNameTest() {
		 String clusterName = elasticSearchManage.getClusterName();
		 assertEquals("elasticsearch-cluster", clusterName);
	}
   /**
    * 获得集群的状态
    */
   @Test
   public void getClusterStatusTest(){
	   String esClusterHealthStatus = elasticSearchManage.getEsClusterHealthStatus();
	   logger.info("esClusterHealthStatus:"+esClusterHealthStatus);
	   assertEquals("GREEN", esClusterHealthStatus);
	   
   }
   
   /**
    * 获得ES集群的DataNode
    */
   @Test
   public void getDataNodeCountTest(){
	   int dataNodeCount = elasticSearchManage.getDataNodeCount();
	   logger.info("dataNodeCount:"+dataNodeCount);
	   assertEquals(1, dataNodeCount);
   }
   
   /**
    *获得集群节点数 
    */
   @Test
   public void getClusterNodeCountTest(){
	   int clusterNodeCount = elasticSearchManage.getClusterNodeCount();
	   assertEquals(1, clusterNodeCount);
   }
   
   /**
    * 别名是否存在
    * 
    */
   @Test
   public void isExistAliasTest(){
	   String aliasName = "test";
	   Boolean existAlias = elasticSearchManage.isExistAlias(aliasName);
	   assertEquals(false, existAlias);
   }
   
   /**
    * 根据索引增加别名
    * @param index
    * @param aliasName
    */
   @Test
   public void addAliasTest(){
	   String indexName = "books";
	   String aliasName = "test123";
	   Boolean result = elasticSearchManage.addAlias(indexName, aliasName);
	   assertEquals(true, result);
   }
   
   /**
    * 根据索引删除别名
    */
   @Test
   public void delAliasTest(){
	   String indexName = "books";
	   String aliasName = "test123";
	   Boolean result = elasticSearchManage.delAlias(indexName, aliasName);
	   assertEquals(true, result);
   }
   
   /**
    * 根据索引更新Setting
    */
   @Test
   public void updateSettingsByIndex(){
	   String indexName = "books";
	   Settings settings = Settings.builder().put("index.number_of_replicas",4).build();
	   Boolean result = elasticSearchManage.updateSettingsByIndex(indexName, settings);
	   assertEquals(true, result);
   }
   
   @Test
   public void checkIndexStatusTest(){
	   String indexName = "books";
	    String status = elasticSearchManage.checkIndexStatus(indexName);
	    assertEquals("OPEN", status);
   }
   
   /**
    * 关闭索引
    */
   @Test
   public void closeIndexByIndexNameTest(){
	   String indexName = "books";
	   Boolean result = elasticSearchManage.closeIndexByIndexName(indexName);
	   assertEquals(true, result);
   }
   
   @Test
   public void openIndexByIndexName(){
	   String indexName = "books";
	   Boolean result = elasticSearchManage.openIndexByIndexName(indexName);
	   assertEquals(true, result);
   }
   
}
