package com.example.elasticsearch.estest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.elasticsearch.vo.BookVO;

/**
* @author wudi
* @version 创建时间：2018年7月22日 下午4:45:32
* @ClassName ElasticsearchTestController
* @Description ElasticsearchTest client 测试
*/
@RestController
@RequestMapping(value = "/estest")
public class ElasticsearchTestController {

	private static Logger logger = Logger.getLogger(ElasticsearchTestController.class);
	@Autowired
	private TransportClient client;
	
	/**
	 * 简单查询语句
	 * @param id
	 * @return
	 * 
	 */
	@GetMapping(value="/get/book/novel/{id}")
	public ResponseEntity get(@PathVariable("id") String id){
		if(id.isEmpty()){
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}
		GetResponse response = this.client.prepareGet("book", "novel", id).get();
		if(!response.isExists()){
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity(response.getSource(), HttpStatus.OK);
	}
	
	/**
	 * 新增一条数据测试
	 * @param bookVO
	 * @return
	 */
	@PostMapping(value="/add/book/novel")
	public ResponseEntity add (BookVO bookVO){
		try {
			XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
					                         .field("title", bookVO.getTitle())
			                                 .field("author", bookVO.getAuthor())
			                                 .field("word_count", bookVO.getWord_count())
			                                 .field("publish_date", bookVO.getPublish_date().getTime())
			                                 .endObject();
		    IndexResponse indexResponse = this.client.prepareIndex("book", "novel").setSource(contentBuilder).get();
		    return new ResponseEntity(indexResponse.getId(),HttpStatus.OK);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("新增es数据出现错误", e);
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
	}
	
	/**
	 * 根据id删除对应的doc
	 * @param id
	 * @return
	 */
	@DeleteMapping(value="/delete/book/novel/{id}")
	public ResponseEntity delete (@PathVariable("id") String id){
		DeleteResponse deleteResponse = this.client.prepareDelete("book", "novel", id).get();
		return new ResponseEntity(deleteResponse.getResult().toString(), HttpStatus.OK);
	}
	
	/**
	 * 根据id编辑对应的doc
	 * @param bookVO
	 * @return
	 */
	@PutMapping(value="/update/book/novel")
	public ResponseEntity update (BookVO bookVO){
		String id  = bookVO.getId();
		UpdateRequest updateRequest = new UpdateRequest("book", "novel", id);
		try {
			XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject();
			if(bookVO.getTitle()!=null){
				contentBuilder.field("title", bookVO.getTitle());
			}
			if(bookVO.getWord_count()!=null){
				contentBuilder.field("word_count", bookVO.getWord_count());
			}
			if(bookVO.getAuthor()!=null){
				contentBuilder.field("author", bookVO.getAuthor());
			}
			if(bookVO.getPublish_date()!=null){
				contentBuilder.field("publish_date", bookVO.getPublish_date().getTime());
			}
			contentBuilder = contentBuilder.endObject();
			updateRequest.doc(contentBuilder);   	                       
			UpdateResponse updateResponse = this.client.update(updateRequest).get();                   
		    return new ResponseEntity(updateResponse.getResult(),HttpStatus.OK);
		} catch (IOException e) {
			logger.error("编辑es数据出现错误IOException", e);
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (InterruptedException e) {
			logger.error("编辑es数据出现错误InterruptedException", e);
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (ExecutionException e) {
			logger.error("编辑es数据出现错误ExecutionException", e);
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
	}
	
	/**
	 * 条件查询
	 * @param bookVO
	 * @return
	 */
	@PostMapping(value="/query/book/novel")
	public ResponseEntity query(BookVO bookVO){
	BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
	if(bookVO.getTitle()!=null){
		boolQuery.must(QueryBuilders.matchQuery("title", bookVO.getTitle()));
	}
	if(bookVO.getAuthor()!=null){
		boolQuery.must(QueryBuilders.matchQuery("author", bookVO.getAuthor()));
	}
	RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("word_count");
	rangeQuery.from(bookVO.getGt_word_count());
	if(bookVO.getLt_word_count()!=null&&bookVO.getLt_word_count()>0){
		rangeQuery.to(bookVO.getLt_word_count());
	}
	boolQuery.filter(rangeQuery);
	SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("book").setTypes("novel")
	                                 .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
	                                 .setQuery(boolQuery)
	                                 .setFrom(0)
	                                 .setSize(10);
	SearchResponse searchResponse = searchRequestBuilder.get();
	List<Map<String,Object>> list = new ArrayList<>();
	for (SearchHit searchHit : searchResponse.getHits()) {
		list.add(searchHit.getSourceAsMap());
	}
		
	return new ResponseEntity(list,HttpStatus.OK);
	}
}
