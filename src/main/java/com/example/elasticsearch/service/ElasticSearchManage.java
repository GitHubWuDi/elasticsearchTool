package com.example.elasticsearch.service;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import com.example.elasticsearch.vo.EsDocVO;


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
	
	/**
	 * 根据索引获得对应的setting信息
	 * @param indexName
	 * @return
	 */
	public String getSetting(String indexName);
	
	/**
	 * 根据index,type,id,field字段创建对应doc
	 * @param indexName
	 * @param type
	 * @param id
	 * @param field
	 * @return
	 */
	public String createDoc(String indexName,String type,String id,Map<String,Object> field);
	
	/**
	 * 根据id删除对应的doc
	 * @param indexName
	 * @param type
	 * @param id
	 * @return
	 */
	public Boolean delDocByIndexName(String indexName,String type,String id);
	
	/**
	 * 删除索引 
	 * @param indexName
	 * @return
	 */
	public boolean delIndexByIndexName(String indexName);
	
	/**
	 * 获得所有索引
	 * @return
	 */
	public String[] getAllIndex();
	
	/**
	 * 所有索引的个数
	 * @return
	 */
	public int getAllIndexCount();
	
	/**
	 * 获得某一个index下面所有的type
	 * @param indexName
	 * @return
	 */
	public List<String> getTypesByIndexName(String indexName);
	
	/**
	 * 获得index所有字段的名称 
	 * @param indexName
	 * @return
	 */
	public Set<String> getAllFieldsByIndexName(String indexName);
	
	/**
	 * 根据index和type获得对应的fields
	 * @param indexName
	 * @param type
	 * @return
	 */
	public Set<String> getAllFieldsByIndexNameAndType(String indexName,String type);
	/**
	 * 根据indexName和type获得doc
	 * @param indexName
	 * @param type
	 */
	public Map<String,Object> getDoc(String indexName, String type,String id);
	
	/**
	 * 根据index，type，queryBuilder进行查询
	 * @param index
	 * @param type
	 * @param queryBuilder
	 * @param sortBuilders
	 * @param start
	 * @param size
	 * @return
	 */
	public SearchResponse getDocs(String index, String type, QueryBuilder queryBuilder,SortBuilder sortBuilder , int start, int size);
	
	/**
	 * 获得集群名称
	 * @return
	 */
	public String getClusterName();
	
	/**
	 * 获得集群状态
	 * @return
	 */
	public String getEsClusterHealthStatus();
	
	/**
	 * 返回ES集群dataNode的个数
	 * @return
	 */
	public int getDataNodeCount();
	
	/**
	 * 返回集群节点数
	 * @return
	 */
	public int getClusterNodeCount();
	
	/**
	 * 是否存在别名
	 * @param aliasName
	 * @return
	 */
	public Boolean isExistAlias(String aliasName);
	
	/**
	 * 增加别名
	 * @param index
	 * @param aliasName
	 */
	public Boolean addAlias(String index,String aliasName); 
	
	/**
	 * 根据索引删除别名
	 * @param index
	 * @param aliasName
	 */
	public Boolean delAlias(String indexName,String aliasName);
	
	/**
	 * 检索索引状态（是否存在）
	 * @param indexName
	 * @return
	 */
	public  Boolean checkEsIndexState(String indexName);
	
	/**
	 * 检查索引是否启动（关闭或者是打开）
	 * @param indexName
	 * @return
	 */
	public String checkIndexStatus(String indexName);
	/**
	 * 关闭索引
	 * @param indexName
	 * @return
	 */
	public Boolean closeIndexByIndexName(String indexName);
	
	
	
	/**
	 * 打开索引(打开关闭索引)
	 * @param indexName
	 * @return
	 */
	public Boolean openIndexByIndexName(String indexName);
	/**
	 * 更新索引设置(更新索引设置，必须在关闭状态下进行更新)
	 * You can't update the settings of index when the index is in open status. You need to close the index and update the settings and open the index.
	 * @param index
	 * @param settings
	 * @return
	 */
	public Boolean updateSettingsByIndex(String indexName, Settings settings);
	
	/**
	 * 批量创建docs
	 * @param indexName
	 * @param type
	 * @param map
	 * @return
	 */
	public String bulkCreateDocs(String indexName, String type,List<EsDocVO> list);
	
}
