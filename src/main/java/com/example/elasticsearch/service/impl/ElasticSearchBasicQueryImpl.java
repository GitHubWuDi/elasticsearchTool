package com.example.elasticsearch.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.elasticsearch.service.ElasticSearchBasicQuery;


@Service
public class ElasticSearchBasicQueryImpl implements ElasticSearchBasicQuery {

	@Autowired
	private TransportClient client;
	
	private static Logger logger = Logger.getLogger(ElasticSearchBasicQueryImpl.class);
	
	@Override
	public List<Map<String, Object>> queryElasticSearch() {
		//Filter查询
		List<Map<String,Object>> list = new ArrayList<>();
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		boolQuery = boolQuery.filter(QueryBuilders.termQuery("author", "wudi"));
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch("books").setTypes("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(boolQuery).setFrom(0).setSize(10);
		SearchResponse searchResponse = searchRequestBuilder.get();
		for (SearchHit searchHit : searchResponse.getHits()) {
			list.add(searchHit.getSourceAsMap());
		}
		return list;
	}

}
