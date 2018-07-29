package com.example.elasticsearch.service.impl;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.example.elasticsearch.service.ElasticSearchManage;
import com.example.elasticsearch.util.ElasticSearchUtil;

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

}
