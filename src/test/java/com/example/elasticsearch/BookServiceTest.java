package com.example.elasticsearch;

import org.springframework.stereotype.Service;

import com.example.elasticsearch.service.ElasticSearchService;
import com.example.elasticsearch.vo.BookVO;

@Service
public class BookServiceTest extends ElasticSearchService<BookVO>  {

	@Override
	public String getIndexName() {
		String indexName = "books";
		return indexName;
	}

	@Override
	public String getType() {
		String type = "test";
		return type;
	}

}
