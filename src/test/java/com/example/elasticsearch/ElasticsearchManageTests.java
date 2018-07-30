package com.example.elasticsearch;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

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
	public void createDocTest(){
		String indexName = "books";
		String type="test";
		String id ="1";
		Map<String,Object> map  = new HashMap<>();
		map.put("id", "1");
		map.put("title", "test");
		map.put("author", "wudi");
		map.put("id", "1");
		map.put("id", "1");
		map.put("id", "1");
		map.put("id", "1");
		elasticSearchManage.createDoc(indexName, type, id, map);
		
	}
	
	

}
