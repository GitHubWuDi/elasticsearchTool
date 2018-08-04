package com.example.elasticsearch.service.impl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
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
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.example.elasticsearch.enums.ResultCodeEnum;
import com.example.elasticsearch.service.ElasticSearchManage;
import com.example.elasticsearch.util.ElasticSearchException;
import com.example.elasticsearch.util.ElasticSearchUtil;
import com.example.elasticsearch.vo.EsDocVO;

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
	private static Map<String, Boolean> indexState = new HashMap<String, Boolean>(); //index状态结合
	private static final String SUCCESS = "success";
	private static final String OPEN = "OPEN"; //索引打开状态
	private static final String CLOSE = "CLOSE"; //索引关闭状态

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
	public Boolean createEsIndex(String indexName, String mapping, int shardCount, int repliceCount, Field[] fileds) {
		Boolean result = isExistEsIndex(indexName);
		if (result) {
			logger.info("索引已经存在，无法创建");
			return false;
		} else {
			XContentBuilder builder = ElasticSearchUtil.getXContentBuilder(fileds);
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
			Map<String, Object> map = mappings.get(type).sourceAsMap();
			return map;
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
	public String getSetting(String indexName) {
		Settings settings = null;
		GetSettingsResponse settingsResponse = client.admin().indices().prepareGetSettings(indexName).get();
		ImmutableOpenMap<String, Settings> map = settingsResponse.getIndexToSettings();
		for (ObjectObjectCursor<String, Settings> cursor : map) {
			settings = cursor.value;
		}
		String settingInfo = settings.toString();
		logger.info("settings:" + settingInfo);
		return settingInfo;
	}

	@Override
	public String createDoc(String indexName, String type, String id, Map<String, Object> field) {
		try {
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
			for (Map.Entry<String, Object> entry : field.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				xContentBuilder.field(key, value);
			}
			xContentBuilder.endObject();
			bulkRequestBuilder.add(client.prepareIndex(indexName, type, id).setSource(xContentBuilder));
			BulkResponse bulkResponse = bulkRequestBuilder.get();
			if (bulkResponse.hasFailures()) {
				String buildFailureMessage = bulkResponse.buildFailureMessage();
				return buildFailureMessage;
			} else {
				return SUCCESS;
			}
		} catch (IOException e) {
			logger.error("创建doc报错", e);
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), ResultCodeEnum.ERROR.getMsg());
		}
	}

	@Override
	public Boolean delDocByIndexName(String indexName, String type, String id) {
		Boolean existEsIndex = isExistEsIndex(indexName);
		if (existEsIndex) {
			client.prepareDelete(indexName, type, id).execute().actionGet();
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
	public SearchResponse getDocs(String index, String type, QueryBuilder queryBuilder,SortBuilder sortBuilder , int start, int size) {
		SearchResponse searchResponse = null;
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
		//筛选条件
		if(queryBuilder!=null){
			searchRequestBuilder.setQuery(queryBuilder);
		}
		//排序
		if(sortBuilder!=null){
			searchRequestBuilder.addSort(sortBuilder);
		}
		searchResponse = searchRequestBuilder.setFrom(start).setSize(size).execute().actionGet();
		return searchResponse;
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
        AliasesExistResponse aliasesExistResponse = client.admin().indices().prepareAliasesExist(aliasName).execute().actionGet();
        boolean exists = aliasesExistResponse.isExists();
		return exists;
	}

	@Override
	public Boolean addAlias(String index, String aliasName) {
         IndicesAliasesResponse indicesAliasesResponse = client.admin().indices().prepareAliases().addAlias(index, aliasName).execute().actionGet();
         Boolean acknowledged = indicesAliasesResponse.isAcknowledged();
		 return acknowledged;
	}

	@Override
	public Boolean delAlias(String indexName,String aliasName) {
		 IndicesAliasesResponse indicesAliasesResponse = client.admin().indices().prepareAliases().removeAlias(indexName, aliasName).execute().actionGet();
		 Boolean acknowledged = indicesAliasesResponse.isAcknowledged();
		 return acknowledged;
	}

	@Override
	public Boolean checkEsIndexState(String indexName) {
        if(!indexState.containsKey(indexName)||!indexState.get(indexName)){
        	indexState.put(indexName, isExistEsIndex(indexName));
        }
		return indexState.get(indexName);
	}
	
	@Override
	public String checkIndexStatus(String indexName) {
		ClusterState cs = client.admin().cluster().prepareState().setIndices(indexName).execute().actionGet().getState();
		IndexMetaData md=  cs.getMetaData().index(indexName);
		String state = md.getState().name();
		return state;
	}
	
	@Override
	public Boolean closeIndexByIndexName(String indexName) {
		String checkIndexStatus = checkIndexStatus(indexName);
		if(checkIndexStatus.equals(OPEN)) {
			CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose(indexName).execute().actionGet();
			boolean acknowledged = closeIndexResponse.isAcknowledged();
			return acknowledged;
		}else {
			logger.info("该索引已经关闭");
            return false;			
		}
	}
	
	@Override
	public Boolean openIndexByIndexName(String indexName) {
		String checkIndexStatus = checkIndexStatus(indexName);
		if(checkIndexStatus.equals(CLOSE)){
			OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen(indexName).execute().actionGet();
			Boolean acknowledged = openIndexResponse.isAcknowledged();
			return acknowledged;
		}else {
			logger.info("该索引已经打开");
			return false;
		}
	}
	
	@Override
	public Boolean updateSettingsByIndex(String indexName, Settings settings) {
		Boolean closeResult = closeIndexByIndexName(indexName);
		if(closeResult){
			UpdateSettingsResponse updateSettingsResponse = client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
			Boolean acknowledged = updateSettingsResponse.isAcknowledged();
			openIndexByIndexName(indexName);
			return acknowledged;
		}else{
			return false;
		}
	}

	@Override
	public String bulkCreateDocs(String indexName, String type,List<EsDocVO> list) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (EsDocVO esDocVO : list) {
        	String idValue = esDocVO.getIdValue();
        	Map<String, Object> map = esDocVO.getMap();
        	bulkRequest.add(client.prepareIndex(indexName, type,idValue).setSource(map));
		}
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        String result = bulkResponse.toString();
		return result;
	}

	


}
