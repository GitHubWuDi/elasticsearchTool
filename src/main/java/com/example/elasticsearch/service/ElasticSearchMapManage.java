package com.example.elasticsearch.service;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.example.elasticsearch.util.page.PageReq;
import com.example.elasticsearch.util.page.PageRes;
import com.example.elasticsearch.util.page.QueryCondition;
import com.example.elasticsearch.vo.SearchField;

/** * 
* @author wudi 
* E‐mail:wudi@vrvmail.com.cn 
* @version 创建时间：2018年8月14日 下午2:21:28 
* 类说明
* ElasticSearchMapManage接口
*/
public interface ElasticSearchMapManage {

	/**
	 * 刷新索引
	 * @param indexName
	 */
	public void refreshIndex(String indexName);
	
	/**
	 * 创建索引
	 * @param indexName
	 * @param map
	 * @return
	 */
	//public Boolean createIndex(String indexName,String type, Map<String, Object> map);
	
	
	/**
	 * 根据indexName，type，id获得对应的doc
	 * @param indexName
	 * @param type
	 * @param id
	 * @return
	 */
	public Map<String,Object> getDoc(String indexName,String type, Serializable id);
	
	/**
	 * 根据 indexName和type获得所有的数据
	 * @param indexName
	 * @param type
	 * @return
	 */
	public List<Map<String,Object>> findAll(String indexName,String type);
	
	/**
	 * 根据 indexName和type获得个数
	 * @param indexName
	 * @param type
	 * @return
	 */
	public long count(String indexName,String type);
	
	/**
	 * 根据 indexName和type筛选条件获得数据
	 * @param indexName
	 * @param type
	 * @param conditions
	 * @return
	 */
	public List<Map<String,Object>> findAll(String indexName,String type,List<QueryCondition> conditions);
	
	/**
	 * 根据 indexName和type筛选条件获得数据
	 * @param indexName
	 * @param type
	 * @param conditions
	 * @return
	 */
	public long count(String indexName,String type,List<QueryCondition> conditions);
	
	/**
	 * 根据 indexName和type筛选条件获得数据(按照字段进行排序)
	 * @param conditions
	 * @param value
	 * @return
	 */
	public List<Map<String,Object>> findAll(String indexName,String type,List<QueryCondition> conditions, String value);
	
	
	/**
	 * 根据排序进行查找
	 * @param indexName
	 * @param type
	 * @param conditions
	 * @param value
	 * @param sort
	 * @return
	 */
	public List<Map<String,Object>> findAll(String indexName,String type,List<QueryCondition> conditions, String value, String sort);

	/**
	 * 分页查询
	 * @param conditions
	 * @param value
	 * @param sort
	 * @param start
	 * @param size
	 * @return
	 */
	public List<Map<String,Object>> findAll(String indexName,String type,List<QueryCondition> conditions, String value, String sort, Integer start, Integer size);


	/**
	 * 分页查询doc
	 * @param pageReq
	 * @param conditions
	 * @return
	 */
	public PageRes<Map<String,Object>> findByPage(String indexName,String type,PageReq pageReq, List<QueryCondition> conditions);


	/**
	 * 统计分组
	 * @param conditions
	 * @param field
	 * @return
	 */
	public  List<Map<String, Object>> queryStatistics(String indexName,String type,List<QueryCondition> conditions, SearchField field);
	
	/**
	 * 保存doc
	 * @param indexName
	 * @param type
	 * @param map
	 * @return
	 */
	public Serializable save(String indexName,String type,Map<String,Object> map,Object id);
	
	/**
	 * 批量保存
	 * @param indexName
	 * @param type
	 * @param list
	 */
	public void addList(String indexName,String type,List<Map<String,Object>> list,Object id);
	
	/**
	 * 根据id删除
	 * @param indexName
	 * @param type
	 * @param id
	 */
	public void deleteById(String indexName,String type,Serializable id);
	
	/**
	 * 批量删除
	 * @param indexName
	 * @param type
	 * @param entities
	 */
	public void deleteList(String indexName,String type,List<Map<String,Object>> entities,Object id);
}
