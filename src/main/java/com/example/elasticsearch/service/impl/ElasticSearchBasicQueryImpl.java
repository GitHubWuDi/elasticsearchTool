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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.elasticsearch.service.ElasticSearchBasicQuery;


@Service
public class ElasticSearchBasicQueryImpl implements ElasticSearchBasicQuery {

	@Autowired
	private TransportClient client;
	
	private static Logger logger = Logger.getLogger(ElasticSearchBasicQueryImpl.class);
	
	@Override
	public List<Map<String, Object>> queryElasticSearchBasic() {
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

	@Override
	public void queryElasticSeachAggregations(String indexName,String type,String aggName,String fieldName) {
		//range查询测试
		//Range
		RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders.range(aggName).field(fieldName).keyed(true).addRange("small", 1000, 2000).addRange("medium", 3000, 5000).addRange("medium", 5000, 10000);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		SearchResponse searchResponse = searchRequestBuilder.addAggregation(rangeAggregationBuilder).execute().actionGet();
		Map<String, Aggregation> asMap = searchResponse.getAggregations().getAsMap();
		logger.info(asMap);
	}

	@Override
	public void queryElasticSearchFilterAggregations(String indexName, String type, String aggName, QueryBuilder termQuery){
		FiltersAggregationBuilder filtersAggregationBuilder = AggregationBuilders.filters(aggName, termQuery);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		SearchResponse searchResponse = searchRequestBuilder.addAggregation(filtersAggregationBuilder).execute().actionGet();
		Map<String, Aggregation> asMap = searchResponse.getAggregations().getAsMap();
		logger.info(asMap);
	}

}
