package com.example.elasticsearch.service;

import static org.mockito.Matchers.contains;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
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

import com.example.elasticsearch.enums.ResultCodeEnum;
import com.example.elasticsearch.exception.ElasticSearchErrorEnum;
import com.example.elasticsearch.exception.ElasticSearchException;
import com.example.elasticsearch.model.PrimaryKey;
import com.example.elasticsearch.util.DateUtil;
import com.example.elasticsearch.util.ElasticSearchUtil;
import com.example.elasticsearch.util.page.PageReq;
import com.example.elasticsearch.util.page.PageRes;
import com.example.elasticsearch.util.page.QueryCondition;
import com.example.elasticsearch.vo.ElasticSearchVO;
import com.example.elasticsearch.vo.EsDocVO;
import com.example.elasticsearch.vo.IndexsInfoVO;
import com.example.elasticsearch.vo.ScrollVO;
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
	@Value("${es.shardCount}")
	private int shardCount;   //分区数
	@Value("${es.repliceCount}")
	private int repliceCount; //副本书

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
	private Boolean isEsIndexExist() {
		String indexName = getIndexName();
		Boolean result = elasticSearchManage.checkEsIndexState(indexName);
		return result;
	}

	/**
	 * 检查索引状态（OPEN OR CLOSE）
	 * 
	 * @return
	 */
	private String checkESIndexState() {
		String indexName = getIndexName();
		String indexStatus = elasticSearchManage.checkIndexStatus(indexName);
		return indexStatus;
	}

	/**
	 * 刷新索引
	 */
	private void refreshIndex() {
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
	private Boolean createIndex(int shardCount, int repliceCount,T entity) {
		Map<String, Class<?>> fieldsConvertMap = getFieldConvertMap(entity);
		String indexName = getIndexName();
		String type = getType();
		Boolean result = elasticSearchManage.createEsIndex(indexName, type, shardCount, repliceCount,fieldsConvertMap,entity);
		return result;
	}

	/**
	 * 将Field转换成Map<String, Class<?>>（特殊的数据map）
	 * @param entity
	 * @return
	 */
	private Map<String, Class<?>> getFieldConvertMap(T entity) {
		Map<String, Class<?>> fieldsConvertMap=new HashMap<>();
		Field[] fields = entity.getClass().getDeclaredFields();
		for (Field field : fields) {
	        field.setAccessible(true);
	        String name = field.getName(); //获得属性
			String typeName = field.getType().getName(); //获得typeName
			try{
			switch (typeName) {
			case "java.util.Map":
		        //获取属性值
		        Map<String,Object> value = (Map<String,Object>)field.get(entity);
		        if(value!=null){
		        	Map<String, Class<?>> fieldMap =ElasticSearchUtil.getFieldMap(value,name);
		        	fieldsConvertMap.putAll(fieldMap);
		        }
				break;
			default:
				fieldsConvertMap.put(name, field.getType());
				break;
			}
			}catch(Exception e) {
				 logger.error("拼接解析出现错误:"+e.getMessage(), e);
				 throw new ElasticSearchException(ElasticSearchErrorEnum.UNSPECIFIED,e);
			}
		}
		return fieldsConvertMap;
	}
	
	
	/**
	 * 获得List对应的Map<String,Class<?>>
	 * @param parameterTypeName
	 * @param key
	 * @param field
	 * @param entity
	 * @return
	 */
	 private Map<String,Class<?>> getMapParamerInfoByList(String parameterTypeName,String key,Field field,T entity){
		 Map<String,Class<?>> map = new HashMap<>();
		 try{
			 switch (parameterTypeName) {
			 case "java.lang.String":
			 case "java.lang.Integer":
			 case "java.lang.Boolean":
			 case "java.lang.FLoat":
			 case "java.lang.Long":
			 case "java.lang.Byte":
			 case "java.lang.Short":
			 case "java.lang.Double":
				 Class<?> basicName = Class.forName(parameterTypeName);
				 map.put(key, basicName);
				 break;
			 case "java.util.Map":
				 List<Map<String,Object>> listMap = (List<Map<String,Object>>)field.get(entity);
				 for (Map<String, Object> childrenMap : listMap) {
					 Map<String, Class<?>> fieldMap =ElasticSearchUtil.getFieldMap(childrenMap,key);
					 map.putAll(fieldMap);
				}
				 break;
			 default: //业务实体
				 Class<?> voName = Class.forName(parameterTypeName);
				 map.put(key, voName);
				 break;
			 }
		 }catch(Exception e){
			 logger.error("List拼接解析出现错误:"+e.getMessage(), e);
			 throw new ElasticSearchException(ElasticSearchErrorEnum.UNSPECIFIED,e);
		 }
		return map;
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
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, null, null,null, 0,
					Integer.MAX_VALUE);
			List<T> list = getResultList(searchResponse);
			return list;
		} else{
			 throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	}
	
	

	/**
	 * 查询对应index-type所有doc的个数
	 * 
	 * @return
	 */
	public long count() {
		if (isEsIndexExist() && checkESIndexState().equals("OPEN") ) {
			String indexName = getIndexName();
			String type = getType();
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, null, null,null, 0, 1);
			long totalHits = searchResponse.getHits().getTotalHits();
			return totalHits;
		} else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
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
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, null,null, 0,
					Integer.MAX_VALUE);
			List<T> list = getResultList(searchResponse);
			return list;
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
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
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, null,null, 0, 1);
			long totalHits = searchResponse.getHits().getTotalHits();
			return totalHits;
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
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
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, sortBuilder,null,
					start, size);
			List<T> list = getResultList(searchResponse);
			return list;
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
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
			SearchResponse searchResponse = elasticSearchManage.getDocs(indexName, type, queryBuilder, sortBuilder,null,
					start_, count_);
			
			PageRes<T> paginationResponse = getPaginationResponse(searchResponse);
			return paginationResponse;
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
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
	 * 复杂分组查询(主要用于图表展示)
	 * @param conditions
	 * @param field
	 * @return
	 */
	public  List<Map<String, Object>> queryStatistics(List<QueryCondition> conditions, SearchField field){
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")){
			String indexName = getIndexName();
			String type = getType();
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
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
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
			String createDoc = savedoc(entity);
			return createDoc;
		}else{
			judgeIndexStatus(entity);
			String result = savedoc(entity);
			return result;
		}
	}

	/**
	 * 判断index的状态（包括是否存在和是否开启）,并进行相应操作
	 */
	private void judgeIndexStatus(T entity) {
		if(isEsIndexExist()==false){
			Boolean createIndex = createIndex(shardCount,repliceCount,entity);
			logger.info("索引创建状态结果："+createIndex);
		}
		if(!checkESIndexState().equals("OPEN")){
			String indexName = getIndexName();
			Boolean openIndexByIndexName = elasticSearchManage.openIndexByIndexName(indexName);
			logger.info("索引开启状态结果："+openIndexByIndexName);
		}
	}

	/**
	 * 保存数据
	 * @param entity
	 * @return
	 */
	private String savedoc(T entity) {
		String indexName = getIndexName();
		String type = getType();
		String id = getIdValue(entity);
		Map<String, Object> map = ElasticSearchUtil.transBean2Map(entity);
		String createDoc = elasticSearchManage.createDoc(indexName, type, id, map);
		refreshIndex();
		return createDoc;
	}
	
	/**
	 * 批量保存数据
	 * @param entities
	 */
	public void addList(List<T> entities){
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")){
			bulkDoc(entities);
		}else{
			if(entities.size()>0){
				judgeIndexStatus(entities.get(0));
				bulkDoc(entities);
				
			}
		}
	}

	/**
	 * 批量保存数据
	 * @param entities
	 */
	private void bulkDoc(List<T> entities) {
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
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
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
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
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
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	}
	
	
	
	/**
	 * scroll进行分页查询(选择对应的索引，第一次获得)
	 * @param indexsInfoVO
	 * @param conditions
	 * @param value
	 * @param sort
	 * @param size
	 * @return
	 */
	public ScrollVO<T> findAll(IndexsInfoVO indexsInfoVO,List<QueryCondition> conditions,String value, String sort,Integer size){
		String[] indexs = indexsInfoVO.getIndex();
		String[] type = indexsInfoVO.getType();
		for (String index : indexs) {
			Boolean index_status = elasticSearchManage.checkEsIndexState(index);
			Boolean existEsIndex = elasticSearchManage.isExistEsIndex(index);
			if(!existEsIndex||!index_status){
				logger.info(index+"索引没有开启或不存在，请检查！");
				throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
			}
		}
		QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
		SortBuilder sortBuilder = null;
		if (StringUtils.isNoneEmpty(value) && StringUtils.isNoneEmpty(sort)) {
			if ("asc".equalsIgnoreCase(sort)) {
				sortBuilder = SortBuilders.fieldSort(value).order(SortOrder.ASC);
			} else {
				sortBuilder = SortBuilders.fieldSort(value).order(SortOrder.DESC);
			}
		}
		SearchResponse searchResponse = elasticSearchManage.getDocsByScroll(indexs,type, queryBuilder, sortBuilder,null,size);
		ScrollVO<T> scrollVO = getScrollResultList(searchResponse);
		return scrollVO;
	}
	
	/**
	 * 根据基本的索引名称获得对应一系列索引集合
	 * @param baseIndexName
	 * @return
	 */
	public String[] getIndexListByBaseIndexName(String baseIndexName){
		String[] allIndex = elasticSearchManage.getAllIndex();
		List<String> list = new ArrayList<>();
		for (String index : allIndex) { //判断每个index,是否包括了baseIndexName
			if(index.contains(baseIndexName)||index.contains(baseIndexName.toLowerCase())){
				list.add(index);
			}
		}
		String[] array = new String[list.size()];
		String[] index_array = list.toArray(array);
		return index_array;
	}
	
	/**
	 * scroll进行分页查询(全索引，第一次获得)
	 * @param conditions
	 * @param value
	 * @param sort
	 * @param size
	 * @return
	 */
	public ScrollVO<T> findAll(List<QueryCondition> conditions, String value, String sort,Integer size){
		if (isEsIndexExist() && checkESIndexState().equals("OPEN")){
			String indexName = getIndexName();
			String type = getType();
			String[] indexNameArr = getIndexListByBaseIndexName(indexName); 
			QueryBuilder queryBuilder = ElasticSearchUtil.toQueryBuilder(conditions);
			SortBuilder sortBuilder = null;
			if (StringUtils.isNoneEmpty(value) && StringUtils.isNoneEmpty(sort)) {
				if ("asc".equalsIgnoreCase(sort)) {
					sortBuilder = SortBuilders.fieldSort(value).order(SortOrder.ASC);
				} else {
					sortBuilder = SortBuilders.fieldSort(value).order(SortOrder.DESC);
				}
			}
			SearchResponse searchResponse = elasticSearchManage.getDocsByScroll(indexNameArr, new String[]{type}, queryBuilder, sortBuilder,null,size);
			ScrollVO<T> scrollVO = getScrollResultList(searchResponse);
			return scrollVO;
		}else{
			throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
		}
	}
	
	
	/**
	 * es获得数据获得ScrollVO数据
	 * @param searchResponse
	 * @return
	 */
	private ScrollVO<T> getScrollResultList(SearchResponse searchResponse){
		ScrollVO<T> scroll = new ScrollVO<>();
		Gson gson = new GsonBuilder().setDateFormat(DateUtil.DEFAULT_DATE_PATTERN).create();
		ElasticSearchVO<T> elasticSearchVO = gson.fromJson(searchResponse.toString(),new TypeToken<ElasticSearchVO<T>>() {
				}.getType());
		List<T> list = elasticSearchVO.getList(clazz);
		String _scroll_id = elasticSearchVO.get_scroll_id();
		long total = elasticSearchVO.getHits().getTotal();
		scroll.setList(list);
		scroll.setScrollId(_scroll_id);
		scroll.setTotal(total);
		return scroll;
	}
	
	/**
	 * 通过scrollId查询游标数据
	 * @param scrollId
	 * @return
	 */
	public ScrollVO<T> searchByScrollId(String scrollId){
		SearchResponse searchResponse = elasticSearchManage.searchByScrollId(scrollId);
		ScrollVO<T> scrollVO = getScrollResultList(searchResponse);
		return scrollVO;
	}
	
	
	/**
	 * 通过ScrollId清除游标
	 * @param scrollId
	 * @return
	 */
	public Boolean cleanScrollId(String scrollId) {
		boolean result = elasticSearchManage.clearScroll(scrollId);
		return result;
	}
	
	
}
