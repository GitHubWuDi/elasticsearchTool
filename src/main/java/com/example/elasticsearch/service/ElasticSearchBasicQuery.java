package com.example.elasticsearch.service;

import java.util.List;
import java.util.Map;

public interface ElasticSearchBasicQuery {
    
	/**
	 * 查询ElasticSearch
	 * @return
	 */
	public List<Map<String,Object>> queryElasticSearch();
}
