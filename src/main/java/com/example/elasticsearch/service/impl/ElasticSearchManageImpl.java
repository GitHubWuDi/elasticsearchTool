package com.example.elasticsearch.service.impl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.example.elasticsearch.enums.FieldType;
import com.example.elasticsearch.exception.ElasticSearchErrorEnum;
import com.example.elasticsearch.exception.ElasticSearchException;
import com.example.elasticsearch.service.ElasticSearchManage;
import com.example.elasticsearch.util.DateUtil;
import com.example.elasticsearch.util.ElasticSearchUtil;
import com.example.elasticsearch.vo.EsDocVO;
import com.example.elasticsearch.vo.RangeVO;
import com.example.elasticsearch.vo.SearchField;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @author wudi
 * @version 创建时间：2018年7月28日 下午3:51:46
 * @ClassName ElasticSearchManageImpl
 * @Description ES底层接口实现类
 */
@Component
public class ElasticSearchManageImpl implements ElasticSearchManage {

	private static Logger logger = Logger.getLogger(ElasticSearchManageImpl.class);
	private static final String number_of_shards = "index.number_of_shards"; // 分区数
	private static final String number_of_replicas = "index.number_of_replicas"; // 副本数
	private static final String max_result_window = "index.max_result_window"; // 最大返回结果数
	private static Map<String, Boolean> indexExistState = new HashMap<String, Boolean>(); // index状态结合(是否存在)
	private static Map<String,String> indexState = new HashMap<String,String>();  //index状态（索引状态）
	private static final String SUCCESS = "success";
	private static final String OPEN = "OPEN"; // 索引打开状态
	private static final String CLOSE = "CLOSE"; // 索引关闭状态

	@Autowired
	private TransportClient client;

	@Override
	public Boolean isExistEsIndex(String index) {
		IndicesExistsRequest request = new IndicesExistsRequest(index);
		IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
		boolean exists = response.isExists();
		return exists;
	}
	
	@Override
	public Boolean isExistEsTypeOfIndex(String indexName, String type) {
		TypesExistsResponse typesExistsResponse = client.admin().indices().typesExists(new TypesExistsRequest(new String[]{indexName}, type)).actionGet();
		Boolean result = typesExistsResponse.isExists();
		return result;
	}

	@Override
	public Boolean createEsIndex(String indexName) {
		Boolean result = isExistEsIndex(indexName);
		if (result) {
			logger.info("索引已经存在，无法创建");
			return false;
		} else {
			CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(indexName).get();
			Boolean acknowledged = createIndexResponse.isAcknowledged();
			logger.info("索引创建情况：" + acknowledged);
			return acknowledged;
		}
	}

	@Override
	public Boolean createEsIndex(String indexName, int shardCount, int repliceCount) {
		Boolean result = isExistEsIndex(indexName);
		if (result) {
			logger.info("索引已经存在，无法创建");
			return false;
		} else {
			Builder settings = Settings.builder().put(number_of_shards, shardCount).put(number_of_replicas,
					repliceCount);
			CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(indexName)
					.setSettings(settings).get();
			Boolean acknowledged = createIndexResponse.isAcknowledged();
			logger.info("索引创建情况：" + acknowledged);
			return acknowledged;
		}
	}

	@Override
	public Boolean createEsIndex(String indexName,String mapping,int shardCount,int repliceCount,Map<String,Class<?>> fields,Object obj) {
		Boolean indexResult = isExistEsIndex(indexName);
		if (indexResult) {
			logger.info("索引和类型已经存在，无法创建");
			return false;
		} else {
			XContentBuilder builder = ElasticSearchUtil.getXContentBuilder(fields,obj);
			Builder settings = Settings.builder().put(number_of_shards, shardCount)
					.put(number_of_replicas, repliceCount).put(max_result_window, Integer.MAX_VALUE);
			CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(indexName)
					.setSettings(settings).addMapping(mapping, builder).get();
			Boolean acknowledged = createIndexResponse.isAcknowledged();
			logger.info("索引创建情况：" + acknowledged);
			return acknowledged;
		}
	}


	@Override
	public void refreshAllIndex() {
		client.admin().indices().prepareRefresh().get();
	}

	@Override
	public void refreshIndexByIndexName(String indexName) {
		client.admin().indices().prepareRefresh(indexName);
	}

	@Override
	public Map<String, Object> getMapping(String indexName, String type) {
		Boolean result = isExistEsIndex(indexName);
		if (result) {
			ImmutableOpenMap<String, MappingMetaData> mappings = client.admin().cluster().prepareState().execute()
					.actionGet().getState().getMetaData().getIndices().get(indexName).getMappings();
			try {
				Map<String, Object> map = mappings.get(type).sourceAsMap();
				return map;
			} catch (IOException e) {
				throw new RuntimeException("获得对应的mapping的值！");
			}
		} else {
			return null;
		}
	}

	@Override
	public boolean setRepliceCountNum(String indexName, int repliceCount) {
		Boolean result = isExistEsIndex(indexName);
		if (result) {
			Builder builder = Settings.builder().put(number_of_replicas, repliceCount);
			UpdateSettingsResponse updateSettingsResponse = client.admin().indices().prepareUpdateSettings(indexName)
					.setSettings(builder).get();
			boolean acknowledged = updateSettingsResponse.isAcknowledged();
			return acknowledged;
		} else {
			return false;
		}
	}

	@Override
	public long getDocCountByIndexName(String indexName) {
		SearchResponse searchResponse = client.prepareSearch(indexName).setFrom(0).execute().actionGet();
		SearchHits hits = searchResponse.getHits();
		long totalHits = hits.totalHits;
		return totalHits;
	}

	@Override
	public int getShardCountByIndexName(String indexName) {
		SearchResponse searchResponse = client.prepareSearch(indexName).setFrom(0).execute().actionGet();
		int totalShards = searchResponse.getTotalShards();
		return totalShards;
	}

	@Override
	public int getSuccessShardCount(String indexName) {
		SearchResponse searchResponse = client.prepareSearch(indexName).setFrom(0).execute().actionGet();
		int successfulShards = searchResponse.getSuccessfulShards();
		return successfulShards;
	}

	@Override
	public int getAllRepliceCount(String indexName) {
		Integer count = 0;
		GetSettingsResponse getSettingsResponse = client.admin().indices().prepareGetSettings(indexName).get();
		ImmutableOpenMap<String, Settings> settings = getSettingsResponse.getIndexToSettings();
		for (ObjectObjectCursor<String, Settings> objectObjectCursor : settings) {
			Settings value = objectObjectCursor.value;
			count = value.getAsInt(number_of_replicas, null);
		}
		return count;
	}

	@Override
	public Settings getSetting(String indexName) {
		Settings settings = null;
		GetSettingsResponse settingsResponse = client.admin().indices().prepareGetSettings(indexName).get();
		ImmutableOpenMap<String, Settings> map = settingsResponse.getIndexToSettings();
		for (ObjectObjectCursor<String, Settings> cursor : map) {
			settings = cursor.value;
		}
		return settings;
	}

	@Override
	public String createDoc(String indexName, String type, String id, Map<String, Object> field) {
		try {
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
			getXcontentBuilder(field, xContentBuilder);
			xContentBuilder.endObject();
			bulkRequestBuilder.add(client.prepareIndex(indexName, type, id).setSource(xContentBuilder)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
			BulkResponse bulkResponse = bulkRequestBuilder.get();
			if (bulkResponse.hasFailures()) {
				String buildFailureMessage = bulkResponse.buildFailureMessage();
				return buildFailureMessage;
			} else {
				return SUCCESS;
			}
		} catch (Exception e) {
			logger.error("创建doc报错", e);
			throw new ElasticSearchException(ElasticSearchErrorEnum.UNSPECIFIED);
		}
	}
	
	/**
	 * 获得对应的XContentBuilder
	 * @param xContentBuilder
	 * @param filedTypeName
	 */
	private void getContextBuilderByList(XContentBuilder xContentBuilder,String filedTypeName,Object value,String key){
		Gson gson = new Gson();
		try{
			switch (filedTypeName) {
			case "java.lang.String":
			case "java.lang.Integer":
			case "int":
			case "java.lang.Boolean":
			case "boolean":
			case "java.lang.FLoat":
			case "float":
			case "java.lang.Long":
			case "long":
			case "java.lang.Byte":
			case "byte":
			case "java.lang.Short":
			case "short":	
			case "java.lang.Double":
			case "double":
				xContentBuilder.field(key, value);
				break;
			case "java.util.ArrayList":
				Field declaredField = value.getClass().getDeclaredField(key);
				String fieldType = ElasticSearchUtil.getParamterTypeByList(declaredField);
				getContextBuilderByList(xContentBuilder, fieldType, value, key);
			case "java.util.Date":
				xContentBuilder.field(key, DateUtil.format((Date)value));
			default:
				xContentBuilder.field(key, value);
				break;
			}
		}catch(Exception e){
			logger.error("getContextBuilderByList报错:"+e.getMessage(), e);
			throw new ElasticSearchException(ElasticSearchErrorEnum.UNSPECIFIED);
		}
	}
	
	
	/**
	 * XcontentBuilder对应方法的修改
	 * @param field
	 * @param xContentBuilder
	 * @throws IOException
	 */
	private void getXcontentBuilder(Map<String, Object> field,
			XContentBuilder xContentBuilder) {
		try{
			for (Map.Entry<String, Object> entry : field.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				if(value!=null){
					String typeName = value.getClass().getTypeName();
					switch (typeName) {
					case "java.lang.String":
					case "java.lang.Integer":
					case "int":
					case "java.lang.Boolean":
					case "boolean":
					case "java.lang.FLoat":
					case "float":
					case "java.lang.Long":
					case "long":
					case "java.lang.Byte":
					case "byte":
					case "java.lang.Short":
					case "short":	
					case "java.lang.Double":
					case "double":
						xContentBuilder.field(key, value);
						break;
					case "java.util.ArrayList":
						//TODO 获得获得List当中的类型
						List valueList = (List)value;
						if(valueList.size()>0){
							String fieldType = valueList.get(0).getClass().getTypeName();
							getContextBuilderByList(xContentBuilder, fieldType, value, key);
						}
						break;
					case "java.util.Date":
						xContentBuilder.field(key, DateUtil.format((Date)value));
						break;
					default:
						Gson gson = new Gson();
						String json = gson.toJson(value);
						Map<String,Object> fromJson = gson.fromJson(json, new TypeToken<Map<String,Object>>(){}.getType());
						xContentBuilder.startObject(key);
						getXcontentBuilder(fromJson,xContentBuilder);
						xContentBuilder.endObject();
						break;
					}
				}
				
			}
		}catch(Exception e){
			logger.error("getXcontentBuilder报错", e);
			throw new ElasticSearchException(ElasticSearchErrorEnum.UNSPECIFIED);
		}
		
	}

	@Override
	public Boolean delDocByIndexName(String indexName, String type, String id) {
		Boolean existEsIndex = isExistEsIndex(indexName);
		if (existEsIndex) {
			client.prepareDelete(indexName, type, id).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).execute().actionGet();
			logger.info("删除doc成功");
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean delIndexByIndexName(String indexName) {
		Boolean existEsIndex = isExistEsIndex(indexName);
		if (existEsIndex) {
			DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(indexName).execute()
					.actionGet();
			boolean acknowledged = deleteIndexResponse.isAcknowledged();
			return acknowledged;
		} else {
			return false;
		}
	}

	@Override
	public String[] getAllIndex() {
		GetIndexResponse getIndexResponse = client.admin().indices().prepareGetIndex().execute().actionGet();
		String[] indices = getIndexResponse.getIndices();
		return indices;
	}

	@Override
	public int getAllIndexCount() {
		String[] allIndex = getAllIndex();
		int length = allIndex.length;
		return length;
	}

	@Override
	public List<String> getTypesByIndexName(String indexName) {
		List<String> list = new ArrayList<>();
		GetMappingsResponse res = null;
		try {
			res = client.admin().indices().getMappings(new GetMappingsRequest().indices(indexName)).get();
		} catch (InterruptedException | ExecutionException t) {
			logger.error("获得type报错", t);
		}
		ImmutableOpenMap<String, MappingMetaData> map = res.getMappings().get(indexName);
		for (ObjectObjectCursor<String, MappingMetaData> objectObjectCursor : map) {
			list.add(objectObjectCursor.key);
		}
		return list;
	}

	@Override
	public Set<String> getAllFieldsByIndexName(String indexName) {
		Set<String> result = new HashSet<>();
		SearchResponse searchResponse = client.prepareSearch(indexName).execute().actionGet();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit searchHit : hits) {
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				result.add(entry.getKey());
			}
		}
		return result;
	}

	@Override
	public Set<String> getAllFieldsByIndexNameAndType(String indexName, String type) {
		Set<String> result = new HashSet<>();
		SearchResponse searchResponse = client.prepareSearch(indexName).setTypes(type).execute().actionGet();
		SearchHits searchHits = searchResponse.getHits();
		for (SearchHit searchHit : searchHits) {
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				result.add(entry.getKey());
			}
		}
		return result;
	}

	@Override
	public Map<String, Object> getDoc(String indexName, String type, String id) {
		GetResponse getResponse = client.prepareGet(indexName, type, id).get();
		Map<String, Object> map = getResponse.getSourceAsMap();
		return map;
	}

	@Override
	public SearchResponse getDocs(String index, String type, QueryBuilder queryBuilder, SortBuilder sortBuilder,
			SearchField field, int start, int size) {
		
		Integer maxResultWindow = getMaxResultWindow(index);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		return selectCondition(queryBuilder, sortBuilder, field, start, size, searchRequestBuilder,maxResultWindow);
	
	}
	
	/**
	 * 根据索引获得最大的ResultWindow
	 * @param indexName
	 * @return
	 */
	private Integer getMaxResultWindow(String[] indexNames) {
		Integer max_result_window = 10000;
		for (String indexName : indexNames) {
			Settings settings = this.getSetting(indexName);
			if(settings!=null&&settings.keySet().contains("index.max_result_window")){
				String setting = settings.get("index.max_result_window");
				Integer indexResultWinodw = Integer.parseInt(setting);
				if(indexResultWinodw>max_result_window){  //取得最大的max_result_window
					max_result_window = indexResultWinodw;
				}
			}
		}
		return max_result_window;
	}
	
	@Override
	public SearchResponse getDocs(String[] index, String[] type, QueryBuilder queryBuilder, SortBuilder sortBuilder,SearchField field, int start, int size){
		Integer maxResultWindow = getMaxResultWindow(index);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		return selectCondition(queryBuilder, sortBuilder, field, start, size, searchRequestBuilder,maxResultWindow);
	}
	
	/**
	 * 筛选分页查询该方式
	 * @param queryBuilder
	 * @param sortBuilder
	 * @param field
	 * @param start
	 * @param size
	 * @param searchRequestBuilder
	 * @return
	 */
	private SearchResponse selectCondition(QueryBuilder queryBuilder, SortBuilder sortBuilder, SearchField field,int start, int size, SearchRequestBuilder searchRequestBuilder,int maxResultWindow) {
		SearchResponse searchResponse = null;
		// 筛选条件
		if (queryBuilder != null) {
			searchRequestBuilder.setQuery(queryBuilder);
		}
		// 排序
		if (sortBuilder != null) {
			searchRequestBuilder.addSort(sortBuilder);
		}
		// 聚合查询
		if (field != null) {
			AggregationBuilder aggregationsBuilder = getAggregationsBuilder(field,maxResultWindow);
			searchRequestBuilder.addAggregation(aggregationsBuilder);
		}
		searchResponse = searchRequestBuilder.setFrom(start).setSize(size).execute().actionGet();
		return searchResponse;
}

	/**
	 * 获得聚合属性
	 * 
	 * @param field
	 * @return
	 */
	private AggregationBuilder getAggregationsBuilder(SearchField field,int maxResultWindow) {
		String fieldName = field.getFieldName(); // 属性名称
		String aggsName = "aggs" + fieldName; // 聚合名称
		FieldType fieldType = field.getFieldType(); // 属性类型
		String timeFormat = field.getTimeFormat(); // 时间格式
		long timeSpan = field.getTimeSpan(); // 时间间隔
		
		DateHistogramInterval timeInterval = field.getTimeInterval();
		List<SearchField> childrenField = field.getChildrenField(); // 子aggs
		AggregationBuilder aggregation = null;
		
		Integer size = field.getSize();
		if(size==null||size.equals(0)) {
			size=maxResultWindow;
		}
		
		switch (fieldType) {
		case Date:
			if(timeSpan!=-1) {
				aggregation = getDateAggregationBuilder(fieldName, aggsName, timeFormat,timeSpan, childrenField);				
			}else {
				aggregation = getDateAggregationBuilder(fieldName, aggsName, timeFormat,timeInterval, childrenField);
			}
			break;
		case String:
		case Object:
			aggregation = AggregationBuilders.terms(aggsName).field(fieldName).size(size);
			break;
		case Range:
			aggregation=getRangeAggregationBuilder(field, fieldName, aggsName);
			break;
		case NumberSum:
			aggregation = AggregationBuilders.sum(aggsName).field(fieldName);
		case NumberAvg:
			aggregation = AggregationBuilders.avg(aggsName).field(fieldName);
		case NumberMax:
			aggregation = AggregationBuilders.max(aggsName).field(fieldName);
			break;
		case NumberMin:
			aggregation = AggregationBuilders.min(aggsName).field(fieldName);
			break;
		case Numberstat:
			aggregation = AggregationBuilders.stats(aggsName).field(fieldName);
			break;
		case ObjectDistinctCount:
			aggregation = AggregationBuilders.cardinality(aggsName).field(fieldName);
			break;
		}
		
		if (childrenField != null && childrenField.size() > 0) {
			for (SearchField searchField : childrenField) {
				if (searchField != null) {
					AggregationBuilder aggregationsBuilder = getAggregationsBuilder(searchField,maxResultWindow);
					aggregation.subAggregation(aggregationsBuilder);
				}
			}
		}
		return aggregation;
	}

	/**
	 * 根据range分段查询
	 * @param field
	 * @param fieldName
	 * @param aggsName
	 * @return
	 */
	private AggregationBuilder getRangeAggregationBuilder(SearchField field, String fieldName, String aggsName) {
		List<RangeVO> rangeList = field.getRangeList();
		RangeAggregationBuilder  rangeAggregation= AggregationBuilders.range(aggsName).field(fieldName).keyed(true);
		for (RangeVO rangeVO : rangeList) {
			String metricName = rangeVO.getMetricName();
			long start = rangeVO.getStart();
			long end = rangeVO.getEnd();
			rangeAggregation.addRange(metricName,start,end);
		}
		return rangeAggregation;
	}


	/**
	 * 根据时间分组进行处理
	 * @param fieldName
	 * @param aggsName
	 * @param timeFormat
	 * @param timeSpan
	 * @param childrenField
	 * @return
	 */
	private AggregationBuilder getDateAggregationBuilder(String fieldName, String aggsName, String timeFormat,
			long timeSpan, List<SearchField> childrenField) {
		DateHistogramAggregationBuilder dateHistogramAggregationBuilder = null;
		if (timeSpan > 0) {
			dateHistogramAggregationBuilder = AggregationBuilders.dateHistogram(aggsName).field(fieldName)
					.interval(timeSpan).format(timeFormat);
		} else {
			dateHistogramAggregationBuilder = AggregationBuilders.dateHistogram(aggsName).field(fieldName)
					.format(timeFormat);
		}
		return dateHistogramAggregationBuilder;
	}
	
	
	private AggregationBuilder getDateAggregationBuilder(String fieldName, String aggsName, String timeFormat,
			DateHistogramInterval timeInterval, List<SearchField> childrenField) {
		DateHistogramAggregationBuilder dateHistogramAggregationBuilder = null;
		if (timeInterval !=null) {
			dateHistogramAggregationBuilder = AggregationBuilders.dateHistogram(aggsName).field(fieldName)
					.dateHistogramInterval(timeInterval).format(timeFormat);
		} else {
			dateHistogramAggregationBuilder = AggregationBuilders.dateHistogram(aggsName).field(fieldName)
					.format(timeFormat);
		}
		return dateHistogramAggregationBuilder;
	}
	

	@Override
	public String getClusterName() {
		ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().get();
		String clusterName = clusterHealthResponse.getClusterName();
		return clusterName;
	}

	@Override
	public String getEsClusterHealthStatus() {
		String clusterHealthStatu = null;
		ClusterHealthResponse response = client.admin().cluster().prepareHealth().get();
		Map<String, ClusterIndexHealth> map = response.getIndices();
		for (Map.Entry<String, ClusterIndexHealth> entry : map.entrySet()) {
			ClusterIndexHealth clusterIndexHealth = entry.getValue();
			ClusterHealthStatus status = clusterIndexHealth.getStatus();
			clusterHealthStatu = status.toString();
		}
		return clusterHealthStatu;
	}

	@Override
	public int getDataNodeCount() {
		ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().get();
		int number = clusterHealthResponse.getNumberOfDataNodes();
		return number;
	}

	@Override
	public int getClusterNodeCount() {
		ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().get();
		int number = clusterHealthResponse.getNumberOfNodes();
		return number;
	}

	@Override
	public Boolean isExistAlias(String aliasName) {
		AliasesExistResponse aliasesExistResponse = client.admin().indices().prepareAliasesExist(aliasName).execute()
				.actionGet();
		boolean exists = aliasesExistResponse.isExists();
		return exists;
	}

	@Override
	public Boolean addAlias(String index, String aliasName) {
		IndicesAliasesResponse indicesAliasesResponse = client.admin().indices().prepareAliases()
				.addAlias(index, aliasName).execute().actionGet();
		Boolean acknowledged = indicesAliasesResponse.isAcknowledged();
		return acknowledged;
	}

	@Override
	public Boolean delAlias(String indexName, String aliasName) {
		IndicesAliasesResponse indicesAliasesResponse = client.admin().indices().prepareAliases()
				.removeAlias(indexName, aliasName).execute().actionGet();
		Boolean acknowledged = indicesAliasesResponse.isAcknowledged();
		return acknowledged;
	}

	@Override
	public Boolean checkEsIndexState(String indexName) {
		if (!indexExistState.containsKey(indexName) || !indexExistState.get(indexName)) {
			indexExistState.put(indexName, isExistEsIndex(indexName));
		}
		return indexExistState.get(indexName);
	}

	@Override
	public String checkIndexStatus(String indexName) {
		if (!indexState.containsKey(indexName) || indexState.get(indexName)==null) {
			indexState.put(indexName, isEsIndexOpen(indexName));
		}
		return indexState.get(indexName);
	}

	/**
	 * ES索引是否开启
	 * @param indexName
	 * @return
	 */
	private String isEsIndexOpen(String indexName) {
		ClusterState cs = client.admin().cluster().prepareState().setIndices(indexName).execute().actionGet()
				.getState();
		IndexMetaData md = cs.getMetaData().index(indexName);
		String state = md.getState().name();
		return state;
	}

	@Override
	public Boolean closeIndexByIndexName(String indexName) {
		String checkIndexStatus = checkIndexStatus(indexName);
		if (checkIndexStatus.equals(OPEN)) {
			CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose(indexName).execute()
					.actionGet();
			boolean acknowledged = closeIndexResponse.isAcknowledged();
			return acknowledged;
		} else {
			logger.info("该索引已经关闭");
			return false;
		}
	}

	@Override
	public Boolean openIndexByIndexName(String indexName) {
		String checkIndexStatus = checkIndexStatus(indexName);
		if (checkIndexStatus.equals(CLOSE)) {
			OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen(indexName).execute().actionGet();
			Boolean acknowledged = openIndexResponse.isAcknowledged();
			return acknowledged;
		} else {
			logger.info("该索引已经打开");
			return false;
		}
	}

	@Override
	public Boolean updateSettingsByIndex(String indexName, Settings settings) {
		Boolean closeResult = closeIndexByIndexName(indexName);
		if (closeResult) {
			UpdateSettingsResponse updateSettingsResponse = client.admin().indices().prepareUpdateSettings(indexName)
					.setSettings(settings).execute().actionGet();
			Boolean acknowledged = updateSettingsResponse.isAcknowledged();
			openIndexByIndexName(indexName);
			return acknowledged;
		} else {
			return false;
		}
	}

	@Override
	public String bulkCreateDocs(String indexName, String type, List<EsDocVO> list) {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		try{
		for (EsDocVO esDocVO : list) {
			String idValue = esDocVO.getIdValue();
			Map<String, Object> map = esDocVO.getMap();
				XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
				getXcontentBuilder(map, xContentBuilder);
				xContentBuilder.endObject();
				bulkRequest.add(client.prepareIndex(indexName, type, idValue).setSource(xContentBuilder)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		  }
		}catch(Exception e){
			logger.error("拼接数据出现问题:"+e.getMessage(), e);
			throw new ElasticSearchException(ElasticSearchErrorEnum.UNSPECIFIED);
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		String result = bulkResponse.toString();
		return result;
	}

	@Override
	public SearchResponse getDocsByScroll(String index, String type, QueryBuilder queryBuilder, SortBuilder sortBuilder,
			SearchField field, int size) {
		Integer maxResultWindow = getMaxResultWindow(index);
		
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		SearchResponse searchResponse = selectConditionByScroll(queryBuilder, sortBuilder, field, size, searchRequestBuilder,maxResultWindow);
		return searchResponse;
	
	}

	private SearchResponse selectConditionByScroll(QueryBuilder queryBuilder, SortBuilder sortBuilder,SearchField field, int size, SearchRequestBuilder searchRequestBuilder, Integer maxResultWindow) {
		SearchResponse searchResponse = null;
		// 筛选条件
		if (queryBuilder != null) {
			searchRequestBuilder.setQuery(queryBuilder);
		}
		// 排序
		if (sortBuilder != null) {
			searchRequestBuilder.addSort(sortBuilder);
		}
		// 聚合查询
		if (field != null) {
			AggregationBuilder aggregationsBuilder = getAggregationsBuilder(field,maxResultWindow);
			searchRequestBuilder.addAggregation(aggregationsBuilder);
		}
		//采用游标获得对应的搜索结果
		searchResponse = searchRequestBuilder.setSize(size).setScroll(TimeValue.timeValueMinutes(60)).execute().actionGet();
		return searchResponse;
    
	}

	private Integer getMaxResultWindow(String index) {
		Integer max_result_window = 10000;
		Settings settings = this.getSetting(index);
		if (settings != null && settings.keySet().contains("index.max_result_window")) {
			String setting = settings.get("index.max_result_window");
			Integer indexResultWinodw = Integer.parseInt(setting);
			if (indexResultWinodw > max_result_window) { // 取得最大的max_result_window
				max_result_window = indexResultWinodw;
			}
		}

		return max_result_window;
	
	}
	
	@Override
	public SearchResponse getDocsByScroll(String[] indexs, String[] types, QueryBuilder queryBuilder,
			SortBuilder sortBuilder, SearchField field, int size) {
        Integer maxResultWindow = getMaxResultWindow(indexs);
 		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexs).setTypes(types).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		SearchResponse searchResponse = selectConditionByScroll(queryBuilder, sortBuilder, field, size, searchRequestBuilder,maxResultWindow);
		return searchResponse;
	}

	@Override
	public SearchResponse searchByScrollId(String scrollId) {
		SearchScrollRequestBuilder searchScrollRequestBuilder = client.prepareSearchScroll(scrollId);
		searchScrollRequestBuilder.setScroll(TimeValue.timeValueMinutes(60));
		SearchResponse searchResponse = searchScrollRequestBuilder.get();
		return searchResponse;
	}

	@Override
	public boolean clearScroll(String scrollId) {
		ClearScrollRequestBuilder clearScrollRequestBuilder = client.prepareClearScroll();
		clearScrollRequestBuilder.addScrollId(scrollId);
		ClearScrollResponse response = clearScrollRequestBuilder.get();
		boolean succeeded = response.isSucceeded();
		return succeeded;
	}

	@Override
	public void reindexTransportData(String sourceIndexName, String destinationIndexName) {
		try{
			BulkByScrollResponse response = new ReindexRequestBuilder(client, ReindexAction.INSTANCE).source(sourceIndexName).destination(destinationIndexName).get();
			logger.info("转移数据成功");			
		}catch(ElasticSearchException e) {
			logger.error("es数据转移失败，原因："+e.getMessage(), e);
		}
		
		
	}

}
