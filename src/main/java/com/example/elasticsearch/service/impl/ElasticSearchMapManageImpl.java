package com.example.elasticsearch.service.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.elasticsearch.enums.ResultCodeEnum;
import com.example.elasticsearch.exception.ElasticSearchErrorEnum;
import com.example.elasticsearch.exception.ElasticSearchException;
import com.example.elasticsearch.service.ElasticSearchManage;
import com.example.elasticsearch.service.ElasticSearchMapManage;
import com.example.elasticsearch.util.DateUtil;
import com.example.elasticsearch.util.ElasticSearchUtil;
import com.example.elasticsearch.util.page.PageReq;
import com.example.elasticsearch.util.page.PageRes;
import com.example.elasticsearch.util.page.QueryCondition;
import com.example.elasticsearch.vo.ElasticSearchVO;
import com.example.elasticsearch.vo.EsDocVO;
import com.example.elasticsearch.vo.SearchField;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/** * 
* @author wudi 
* E‐mail:wudi@vrvmail.com.cn 
* @version 创建时间：2018年8月14日 下午2:19:32 
* 类说明 
* 关于ES普通类说明
* 
*/
@Service
public class ElasticSearchMapManageImpl implements ElasticSearchMapManage {

	private static Logger logger = Logger.getLogger(ElasticSearchMapManageImpl.class);
	private Gson gson = new Gson();
	@Autowired
	private ElasticSearchManage elasticSearchManage;
	@Value("${es.shardCount}")
	private int shardCount;   //分区数
	@Value("${es.repliceCount}")
	private int repliceCount; //副本书
	
	/**
	 * 查看索引是否存在
	 * 
	 * @return
	 */
	private Boolean isEsIndexExist(String indexName) {
		Boolean result = elasticSearchManage.checkEsIndexState(indexName);
		return result;
	}

	/**
	 * 检查索引状态（OPEN OR CLOSE）
	 * 
	 * @return
	 */
	private String checkESIndexState(String indexName) {
		String indexStatus = elasticSearchManage.checkIndexStatus(indexName);
		return indexStatus;
	}
	
	
	
	@Override
	public void refreshIndex(String indexName) {
		elasticSearchManage.refreshIndexByIndexName(indexName);		
	}

	

	private  Map<String,Class<?>> getFieldMap(Map<String, Object> map) {
		Map<String,Class<?>> field = new HashMap<>();
		for (Map.Entry<String,Object> entry : map.entrySet()){
			String key = entry.getKey();
			Class<? extends Object> fieldClass = entry.getValue().getClass();
			field.put(key, fieldClass);
		}
		return field;
	}
	
	
	private Boolean createIndex(String indexName,String type, Map<String, Object> map) {
		Map<String, Class<?>> fieldMap = getFieldMap(map);
		Boolean result = elasticSearchManage.createEsIndex(indexName, type, shardCount, repliceCount, fieldMap,map);
		return result;
	}

	@Override
	public Map<String, Object> getDoc(String indexName, String type, Serializable id) {
		if (id == null) {
			return null;
		} else {
			Map<String, Object> map = elasticSearchManage.getDoc(indexName, type, id.toString());
			return map;
		}
	}

	@Override
	public List<Map<String, Object>> findAll(String indexName, String type) {
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN") ) {
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, null, null,null, 0,
					Integer.MAX_VALUE);
			List<Map<String,Object>> list = getResultList(searchResponse);
			return list;
		} else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	
	}
	
	/**
	 * ES获得数据格式化
	 * 
	 * @param searchResponse
	 * @return
	 */
	private List<Map<String,Object>> getResultList(SearchResponse searchResponse) {
		Gson gson = new GsonBuilder().setDateFormat(DateUtil.DEFAULT_DATE_PATTERN).create();
		ElasticSearchVO<Map<String,Object>> elasticSearchVO = gson.fromJson(searchResponse.toString(),
				new TypeToken<ElasticSearchVO<Map<String,Object>>>() {
				}.getType());
		List<Map<String, Object>> list = elasticSearchVO.getList();
		return list;
	}

	@Override
	public long count(String indexName, String type) {
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN") ) {
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, null, null,null, 0, 1);
			long totalHits = searchResponse.getHits().getTotalHits();
			return totalHits;
		} else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	
	}

	@Override
	public List<Map<String, Object>> findAll(String indexName, String type, List<QueryCondition> conditions) {
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN")) {
			QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, null,null, 0,
					Integer.MAX_VALUE);
			List<Map<String, Object>> list = getResultList(searchResponse);
			return list;
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	}

	@Override
	public long count(String indexName, String type, List<QueryCondition> conditions) {
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN")) {
			QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, null,null, 0, 1);
			long totalHits = searchResponse.getHits().getTotalHits();
			return totalHits;
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	}

	@Override
	public List<Map<String, Object>> findAll(String indexName, String type, List<QueryCondition> conditions,
			String value) {
		return findAll(indexName,type,conditions, value, "asc");
	}

	@Override
	public List<Map<String, Object>> findAll(String indexName, String type, List<QueryCondition> conditions,
			String value, String sort) {
		List<Map<String, Object>> list = findAll(indexName,type,conditions, value, sort, 0, Integer.MAX_VALUE);
		return list;
	}

	@Override
	public List<Map<String, Object>> findAll(String indexName, String type, List<QueryCondition> conditions,
			String value, String sort, Integer start, Integer size) {
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN")) {
			QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
			SortBuilder sortBuilder = null;
			if (StringUtils.isNoneEmpty(value) && StringUtils.isNoneEmpty(sort)) {
				if ("asc".equalsIgnoreCase(sort)) {
					sortBuilder = SortBuilders.fieldSort(value).order(SortOrder.ASC);
				} else {
					sortBuilder = SortBuilders.fieldSort(value).order(SortOrder.DESC);
				}
			}
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, sortBuilder,null,
					start, size);
			List<Map<String, Object>> list = getResultList(searchResponse);
			return list;
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	
	}

	@Override
	public PageRes<Map<String, Object>> findByPage(String indexName, String type, PageReq pageReq,
			List<QueryCondition> conditions) {
		String by_ = pageReq.getBy_();
		Integer start_ = pageReq.getStart_();
		Integer count_ = pageReq.getCount_();
		String order_ = pageReq.getOrder_();
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN")) {
			QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
			SortBuilder sortBuilder = null;
			if (StringUtils.isNoneEmpty(by_) && StringUtils.isNoneEmpty(order_)) {
				if ("asc".equalsIgnoreCase(order_)) {
					sortBuilder = SortBuilders.fieldSort(by_).order(SortOrder.ASC);
				} else {
					sortBuilder = SortBuilders.fieldSort(by_).order(SortOrder.DESC);
				}
			}
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, sortBuilder,null,
					start_, count_);
			
			PageRes<Map<String, Object>> paginationResponse = getPaginationResponse(searchResponse);
			return paginationResponse;
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	}
	
	private  PageRes<Map<String, Object>> getPaginationResponse(SearchResponse searchResponse){
		Gson gson = new GsonBuilder().setDateFormat(DateUtil.DEFAULT_DATE_PATTERN).create();
		ElasticSearchVO<Map<String, Object>> elasticSearchVO = gson.fromJson(searchResponse.toString(),
				new TypeToken<ElasticSearchVO<Map<String, Object>>>() {
				}.getType());
		PageRes<Map<String, Object>> paginationResponse = elasticSearchVO.toPaginationResponse();
		return paginationResponse;
	}

	@Override
	public List<Map<String, Object>> queryStatistics(String indexName, String type, List<QueryCondition> conditions,
			SearchField field) {
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN")){
			QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, null, field, 0, Integer.MAX_VALUE);
			String fieldName = "aggs" +field.getFieldName();// 聚合名称
			Aggregation aggregation = searchResponse.getAggregations().get(fieldName);
			logger.info(aggregation);
			List<Map<String,Object>> list = ElasticSearchUtil.getMultiBucketsMap(field, aggregation);
			return list;
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	}

	@Override
	public Serializable save(String indexName, String type, Map<String, Object> map,Object id) {
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN")){
			String createDoc = savedoc( indexName,  type, map,id);
			return createDoc;
		}else{
			judgeIndexStatus(indexName, type,map);
			String result = savedoc(indexName,type,map,id);
			return result;
		}
	}
	
	/**
	 * 保存数据
	 * @param entity
	 * @return
	 */
	private String savedoc(String indexName, String type,Map<String, Object> map,Object id) {
		String createDoc = elasticSearchManage.createDoc(indexName, type, id.toString(), map);
		refreshIndex(indexName);
		return createDoc;
	}
	
	/**
	 * 判断index的状态（包括是否存在和是否开启）,并进行相应操作
	 */
	private void judgeIndexStatus(String indexName,String type,Map<String,Object> map) {
		if(isEsIndexExist(indexName)==false){
			Boolean createIndex = createIndex(indexName, type, map);
			logger.info("索引创建状态结果："+createIndex);
		}
		if(!checkESIndexState(indexName).equals("OPEN")){
			Boolean openIndexByIndexName = elasticSearchManage.openIndexByIndexName(indexName);
			logger.info("索引开启状态结果："+openIndexByIndexName);
		}
	}

	@Override
	public void addList(String indexName, String type, List<Map<String, Object>> list,Object id) {
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN")){
			bulkDoc(indexName,type,list,id);
		}else{
			if(list.size()>0){
				Map<String, Object> map = list.get(0);
				judgeIndexStatus(indexName, type,map);
				bulkDoc(indexName,type,list,id);
			}
		}
	}

	private void bulkDoc(String indexName, String type,List<Map<String,Object>> entities,Object id) {
		List<EsDocVO> list = new ArrayList<>();
		for (Map<String,Object> entity : entities) {
			EsDocVO esDocVO = new EsDocVO();
			String idValue = entity.get(id).toString();
			esDocVO.setIdValue(idValue);
			esDocVO.setMap(entity);
			list.add(esDocVO);
		}
		String bulkCreateDocs = elasticSearchManage.bulkCreateDocs(indexName, type, list);
		refreshIndex(indexName);
		logger.info("bulk Message info:"+bulkCreateDocs);
	}
	
	@Override
	public void deleteById(String indexName, String type, Serializable id) {
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN")){
			Boolean result = elasticSearchManage.delDocByIndexName(indexName, type, id.toString());
			logger.info("result:"+result);
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	
		
	}

	@Override
	public void deleteList(String indexName, String type, List<Map<String, Object>> entities,Object id) {
		if (isEsIndexExist(indexName) && checkESIndexState(indexName).equals("OPEN")){
			for (Map<String, Object> map : entities) {
				String idValue = map.get(id).toString();
				deleteById(indexName,type,idValue);
			}
		refreshIndex(indexName);
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	}
	
}
