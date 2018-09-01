package com.example.elasticsearch.service;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.QueryBuilder;

public interface ElasticSearchBasicQuery {
    
	/**
	 * 基础查询查询ElasticSearch
	 * @return
	 */
	public List<Map<String,Object>> queryElasticSearchBasic();
	
	
	/**
	 * es聚合查询
	 * @return
	 */
	public void queryElasticSeachAggregations(String indexName,String type,String aggName,String fieldName);
	
	
	/**
	 * FilterAggregations查询
	 * @param indexName
	 * @param type
	 * @param aggName
	 * @param fieldName
	 */
	public void queryElasticSearchFilterAggregations(String indexName, String type, String aggName, QueryBuilder termQuery);
}
