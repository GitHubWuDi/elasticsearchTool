package com.example.elasticsearch;

import java.util.List;
import java.util.Map;

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
	
	

}
