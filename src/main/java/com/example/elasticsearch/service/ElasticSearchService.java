package com.example.elasticsearch.service;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;

import com.example.elasticsearch.enums.ResultCodeEnum;
import com.example.elasticsearch.model.PrimaryKey;
import com.example.elasticsearch.util.DateUtil;
import com.example.elasticsearch.util.ElasticSearchException;
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

/**
 * @author wudi
 * @version 创建时间：2018年7月31日 下午10:38:46
 * @ClassName ElasticSearchService
 * @Description ES相关业务实现类(标准：不在该类中使用elasticsearch原生API)
 */
public abstract class ElasticSearchService<T> {

	private static Logger logger = Logger.getLogger(ElasticSearchService.class);
	private Gson gson = new Gson();
	@Autowired
	private ElasticSearchManage elasticSearchManage;

	protected Class<T> clazz;
	protected String idField;

	/**
	 * 构造函数
	 */
	public ElasticSearchService() {
		// 反射得到T的真实类型
		ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass(); // 获取当前new的对象的泛型的父类的类型
		this.clazz = (Class<T>) parameterizedType.getActualTypeArguments()[0]; // 获取第一个类型参数的真实类型model,clazz.newInstance();实例化需要的时候添加
		this.idField = clazz.getAnnotation(PrimaryKey.class).value();
	}

	/**
	 * 获得索引名称
	 * 
	 * @return
	 */
	public abstract String getIndexName();

	/**
	 * 获得索引type
	 * 
	 * @return
	 */
	public abstract String getType();

	/**
	 * 查看索引是否存在
	 * 
	 * @return
	 */
	public Boolean isEsIndexExist() {
		String indexName = getIndexName();
		Boolean result = elasticSearchManage.checkEsIndexState(indexName);
		return result;
	}

	/**
	 * 检查索引状态（OPEN OR CLOSE）
	 * 
	 * @return
	 */
	public String checkESIndexState() {
		String indexName = getIndexName();
		String indexStatus = elasticSearchManage.checkIndexStatus(indexName);
		return indexStatus;
	}

	/**
	 * 刷新索引
	 */
	public void refreshIndex() {
		String indexName = getIndexName();
		elasticSearchManage.refreshIndexByIndexName(indexName);
	}

	/**
	 * 创建索引
	 * 
	 * @param shardCount
	 * @param repliceCount
	 * @return
	 */
	public Boolean createIndex(int shardCount, int repliceCount) {
		Field[] fields = clazz.getDeclaredFields();
		String indexName = getIndexName();
		String type = getType();
		Boolean result = elasticSearchManage.createEsIndex(indexName, type, shardCount, repliceCount, fields);
		return result;
	}

	/**
	 * 根据id获得对应的doc
	 * 
	 * @param id
	 * @return
	 */
	public T getDoc(Serializable id) {
		if (id == null) {
			return null;
		} else {
			String indexName = getIndexName();
			String type = getType();
			Map<String, Object> map = elasticSearchManage.getDoc(indexName, type, id.toString());
			String json = gson.toJson(map);
			T t = gson.fromJson(json, clazz);
			return t;
		}
	}

	/**
	 * 查找全部Doc
	 * 
	 * @return
	 */
	public List<T> findAll() {
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")) {
			String indexName = getIndexName();
			String type = getType();
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, null, null, 0,
					Integer.MAX_VALUE);
			List<T> list = getResultList(searchResponse);
			return list;
		} else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
	}

	/**
	 * 查询对应index-type所有doc的个数
	 * 
	 * @return
	 */
	public long count() {
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")) {
			String indexName = getIndexName();
			String type = getType();
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, null, null, 0, 1);
			long totalHits = searchResponse.getHits().getTotalHits();
			return totalHits;
		} else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
	}

	/**
	 * 筛选查询doc数
	 * 
	 * @param conditions
	 * @return
	 */
	public List<T> findAll(List<QueryCondition> conditions) {
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")) {
			String indexName = getIndexName();
			String type = getType();
			QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, null, 0,
					Integer.MAX_VALUE);
			List<T> list = getResultList(searchResponse);
			return list;
		}else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
	}

	/**
	 * 根据筛选获得个数
	 * 
	 * @param conditions
	 * @return
	 */
	public long count(List<QueryCondition> conditions) {
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")) {
			String indexName = getIndexName();
			String type = getType();
			QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, null, 0, 1);
			long totalHits = searchResponse.getHits().getTotalHits();
			return totalHits;
		}else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
	}

	/**
	 * 带排序字段的查询条件 默认正序
	 */
	public List<T> findAll(List<QueryCondition> conditions, String value) {
		return findAll(conditions, value, "asc");
	}

	/**
	 * 根据排序进行查找
	 * 
	 * @param conditions
	 * @param value
	 * @param sort
	 * @return
	 */
	public List<T> findAll(List<QueryCondition> conditions, String value, String sort) {
		List<T> list = findAll(conditions, value, sort, 0, Integer.MAX_VALUE);
		return list;
	}

	/**
	 * 分页条件查询
	 * 
	 * @param conditions
	 * @param value
	 * @param sort
	 * @param start
	 * @param size
	 * @return
	 */
	public List<T> findAll(List<QueryCondition> conditions, String value, String sort, Integer start, Integer size) {
		String indexName = getIndexName();
		String type = getType();
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")) {
			QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
			SortBuilder sortBuilder = null;
			if (StringUtils.isNoneEmpty(value) && StringUtils.isNoneEmpty(sort)) {
				if ("asc".equalsIgnoreCase(sort)) {
					sortBuilder = SortBuilders.fieldSort(value).order(SortOrder.ASC);
				} else {
					sortBuilder = SortBuilders.fieldSort(value).order(SortOrder.DESC);
				}
			}
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, sortBuilder,
					start, size);
			List<T> list = getResultList(searchResponse);
			return list;
		}else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
	}

	/**
	 * ES获得数据格式化
	 * 
	 * @param searchResponse
	 * @return
	 */
	private List<T> getResultList(SearchResponse searchResponse) {
		Gson gson = new GsonBuilder().setDateFormat(DateUtil.DEFAULT_DATE_PATTERN).create();
		ElasticSearchVO<T> elasticSearchVO = gson.fromJson(searchResponse.toString(),
				new TypeToken<ElasticSearchVO<T>>() {
				}.getType());
		List<T> list = elasticSearchVO.getList(clazz);
		return list;
	}

	/**
	 * 分页查询doc
	 * 
	 * @param pageReq
	 * @param conditons
	 * @return
	 */
	public PageRes<T> findByPage(PageReq pageReq, List<QueryCondition> conditions) {
		String by_ = pageReq.getBy_();
		Integer start_ = pageReq.getStart_();
		Integer count_ = pageReq.getCount_();
		String order_ = pageReq.getOrder_();
		String indexName = getIndexName();
		String type = getType();
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")) {
			QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
			SortBuilder sortBuilder = null;
			if (StringUtils.isNoneEmpty(by_) && StringUtils.isNoneEmpty(order_)) {
				if ("asc".equalsIgnoreCase(order_)) {
					sortBuilder = SortBuilders.fieldSort(by_).order(SortOrder.ASC);
				} else {
					sortBuilder = SortBuilders.fieldSort(by_).order(SortOrder.DESC);
				}
			}
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, sortBuilder,
					start_, count_);
			
			PageRes<T> paginationResponse = getPaginationResponse(searchResponse);
			return paginationResponse;
		}else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
	}
	
	/**
	 * 获得分页数据（VAP数据类型分页）
	 * @param searchResponse
	 * @return
	 */
	private  PageRes<T> getPaginationResponse(SearchResponse searchResponse){
		Gson gson = new GsonBuilder().setDateFormat(DateUtil.DEFAULT_DATE_PATTERN).create();
		ElasticSearchVO<T> elasticSearchVO = gson.fromJson(searchResponse.toString(),
				new TypeToken<ElasticSearchVO<T>>() {
				}.getType());
		PageRes<T> paginationResponse = elasticSearchVO.toPaginationResponse(clazz);
		return paginationResponse;
	}

	/**
	 * 复杂分组查询
	 * @param conditions
	 * @param field
	 * @return
	 */
	public Map<String,Object> queryStatistics(List<QueryCondition> conditions, SearchField field){
		//TODO 
		return null;
		
	}
	
	/**
	 * 获得doc的主键
	 * @param entity
	 * @return
	 */
	private String getIdValue(T entity) {
		String idValue = null;
		try {
			idValue = BeanUtils.getProperty(entity, idField);
		} catch (Exception  e) {
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "未配置的PrimaryKey依赖，请配置");
		}
		return idValue;
	}
	/**
	 * 保存业务数据
	 * @param entity
	 * @return
	 */
	public Serializable save(T entity){
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")){
			String indexName = getIndexName();
			String type = getType();
			String id = getIdValue(entity);
			Map<String, Object> map = ElasticSearchUtil.transBean2Map(entity);
			String createDoc = elasticSearchManage.createDoc(indexName, type, id, map);
			refreshIndex();
			return createDoc;
		}else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
		
	}
	
	/**
	 * 批量保存数据
	 * @param entities
	 */
	public void addList(List<T> entities){
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")){
			List<EsDocVO> list = new ArrayList<>();
			String indexName = getIndexName();
			String type = getType();
			for (T entity : entities) {
				EsDocVO esDocVO = new EsDocVO();
				String idValue = getIdValue(entity);
				Map<String, Object> map = ElasticSearchUtil.transBean2Map(entity);
				esDocVO.setIdValue(idValue);
				esDocVO.setMap(map);
				list.add(esDocVO);
			}
			String bulkCreateDocs = elasticSearchManage.bulkCreateDocs(indexName, type, list);
			refreshIndex();
			logger.info("bulk Message info:"+bulkCreateDocs);
		}else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
	}
	
	/**
	 * 根据id删除doc
	 * @param id
	 */
	public void deleteById(Serializable id){
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")){
			String indexName = getIndexName();
			String type = getType();
			Boolean result = elasticSearchManage.delDocByIndexName(indexName, type, id.toString());
			logger.info("result:"+result);
		}else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
	}
	
	/**
	 * 以实体删除doc
	 * @param entity
	 */
	public void delete(T entity){
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")){
			String idValue = getIdValue(entity);
			deleteById(idValue);
		}else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
	}
	
	/**
	 * 批量删除doc
	 * @param entities
	 */
	public void deleteList(List<T> entities){
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")){
			for (T t : entities) {
				String idValue = getIdValue(t);
				deleteById(idValue);
			}
		refreshIndex();
		}else{
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "请检查索引是否存在或状态");
		}
	}
	
}
