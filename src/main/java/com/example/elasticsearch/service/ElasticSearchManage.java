package com.example.elasticsearch.service;

import java.lang.reflect.Field;
import java.util.Map;

/**
* @author wudi
* @version 创建时间：2018年7月28日 下午3:49:48
* @ClassName ElasticSearchManage
* @Description ElasticSearch底层接口
*/
public interface ElasticSearchManage {

	/**
	 * 判断集群索引是否存在
	 * @param index
	 * @return
	 */
	public Boolean isExistEsIndex(String index);
	
	/**
	 * 根据索引名称直接创建索引
	 * Mapping当中没有数据
	 * @param indexName
	 */
	public Boolean createEsIndex(String indexName);
	
	/**
	 * 创建索引-属性创建索引（包含index，shardCount，repliceCount）
	 * @param indexName
	 * @param shardCount
	 * @param repliceCount
	 * @return
	 */
	public Boolean createEsIndex(String indexName,int shardCount,int repliceCount);
	
	/**
	 * 创建索引-属性创建索引（包含index，mapping，shardCount，repliceCount，fileds）
	 * @param indexName
	 * @param mapping
	 * @param shardCount
	 * @param repliceCount
	 * @param fileds
	 * @return
	 */
	public Boolean createEsIndex(String indexName,String mapping,int shardCount,int repliceCount,Field[] fileds);
	
	/**
	 * 刷新所有的索引
	 * @return
	 */
	public void refreshAllIndex();
	
	/**
	 * 刷新指定索引-通过索引名称
	 * @param indexName
	 */
	public void refreshIndexByIndexName(String indexName);
	
	/**
	 * 根据indexName和type获得map
	 * @param indexName
	 * @param type
	 * @return
	 */
	public Map<String,Object> getMapping(String indexName,String type);

	/**
	 * 根据对应的索引重新设置副本数
	 * @param indexName
	 * @param repliceCount
	 */
	public boolean setRepliceCountNum(String indexName,int repliceCount);
	
	/**
	 * 索引文档库总数
	 * @param indexName
	 * @return
	 */
	public long getDocCountByIndexName(String indexName);
	
	/**
	 * 根据索引获得总分片数
	 * @param indexName
	 * @return
	 */
	public int getShardCountByIndexName(String indexName);
	
	/**
	 * 获得索引成功启动的分片总数
	 * @param indexName
	 * @return
	 */
	public int getSuccessShardCount(String indexName);
	
	/**
	 * 获得索引库副本总数
	 * @param indexName
	 * @return
	 */
	public int getAllRepliceCount(String indexName);
}
