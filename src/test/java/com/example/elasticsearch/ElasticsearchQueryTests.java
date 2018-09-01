package com.example.elasticsearch;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.elasticsearch.service.ElasticSearchBasicQuery;
import com.example.elasticsearch.service.ElasticSearchManage;
import com.example.elasticsearch.util.ElasticSearchUtil;
import com.example.elasticsearch.vo.BookVO;

import ch.qos.logback.core.net.SyslogOutputStream;

/**
 * @author wudi
 * @version 创建时间：2018年7月28日 下午3:51:46
 * @ClassName ElasticsearchManageTests
 * @Description ES底层接口实现类单元测试
 */

@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticsearchQueryTests {

	private static Logger logger = Logger.getLogger(ElasticsearchQueryTests.class);
	public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

	@Autowired
	private ElasticSearchBasicQuery elasticSearchBasicQuery;

	@Test
	public void elasticQuerySearch(){
		List<Map<String,Object>> queryElasticSearch = elasticSearchBasicQuery.queryElasticSearchBasic();
		logger.info(queryElasticSearch.size());
	}
   
	@Test
	public void elasticQuerySearchAggregations(){
		elasticSearchBasicQuery.queryElasticSeachAggregations("books", "test", "range Aggs", "word_count");
	}
	
	@Test
	public void elasticQuerySearchFilterAggregations(){
		TermQueryBuilder termQuery = QueryBuilders.termQuery("author", "wudi");
		elasticSearchBasicQuery.queryElasticSearchFilterAggregations("books", "test", "filter Aggs", termQuery);
	}
}
