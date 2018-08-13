package com.example.elasticsearch;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.elasticsearch.enums.FieldType;
import com.example.elasticsearch.service.test.BookServiceTest;
import com.example.elasticsearch.util.page.PageReq;
import com.example.elasticsearch.util.page.PageRes;
import com.example.elasticsearch.util.page.QueryCondition;
import com.example.elasticsearch.vo.BookVO;
import com.example.elasticsearch.vo.SearchField;

/**
 * @author wudi
 * @version 创建时间：2018年7月28日 下午3:51:46
 * @ClassName ElasticsearchServiceTests
 * @Description ES业务类实现类单元测试
 */

@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticsearchServiceTests {

	private static Logger logger = Logger.getLogger(ElasticsearchServiceTests.class);
	public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

	@Autowired
	private BookServiceTest bookServiceTest;

	/**
	 * 刷新索引  
	 */
	@Test
	public void refreshIndexTest(){
		bookServiceTest.refreshIndex();
		assertEquals(true,true);
	}
	
	/**
	 *创建索引
	 */
	@Test
	public void createIndexTest(){
		int shardCount =5;
		int repliceCount =1;
		Boolean result = bookServiceTest.createIndex(shardCount, repliceCount);
		assertEquals(true,result);
	}
	
	/**
	 * 通过id获得doc信息
	 *
	 */
	@Test
	public void getDocTest(){
		String id = "1";
		BookVO bookVO = bookServiceTest.getDoc(id);
		String author = bookVO.getAuthor();
		assertEquals("wudi", author);
	}
	
	/**
	 * 查找所有Doc
	 */
	@Test
	public void findAllTest(){
		List<BookVO> list = bookServiceTest.findAll();
		assertEquals(3, list.size());
	}
	
	/**
	 * 查找所有doc的个数
	 */
	@Test
	public void countAllTest(){
		long count = bookServiceTest.count();
		assertEquals(3, count);
	}
	
	/**
	 * 条件查询doc数
	 */
	@Test
	public void findAllConditionTest(){
		List<QueryCondition> conditions = new ArrayList<>();
		conditions.add(QueryCondition.eq("author", "wudi"));
		conditions.add(QueryCondition.eq("title", "浪潮之巅"));
		List<BookVO> list = bookServiceTest.findAll(conditions);
		assertEquals(1, list.size());
	}
	
	/**
	 * 条件查询个数
	 */
	@Test
	public void countConditionTest(){
		List<QueryCondition> conditions = new ArrayList<>();
		conditions.add(QueryCondition.eq("author", "wudi"));
		conditions.add(QueryCondition.eq("title", "浪潮之巅"));
		long count = bookServiceTest.count(conditions);
		assertEquals(1, count);
		
	}
	/**
	 * 排序查询
	 */
	@Test
	public void findAllSortTest(){
		List<QueryCondition> conditions = new ArrayList<>();
		List<BookVO> list = bookServiceTest.findAll(conditions, "publish_date", "asc");
		assertEquals(3, list.size());
	}
   
	/**
	 * Doc分页查询测试
	 */
	@Test
	public void findByPageTest(){
		List<QueryCondition> conditions = new ArrayList<>();
		conditions.add(QueryCondition.eq("author", "wudi"));
		conditions.add(QueryCondition.eq("title", "浪潮之巅"));
		PageReq pageReq = new PageReq();
		pageReq.setBy_("publish_date");
		pageReq.setOrder_("asc");
		pageReq.setCount_(40);
		pageReq.setStart_(0);
		PageRes<BookVO> pageRes = bookServiceTest.findByPage(pageReq, conditions);
		Long total = pageRes.getTotal();
	}
	
	/**
	 * 保存doc
	 */
	@Test
	public void saveDoc(){
		BookVO bookVO = new BookVO();
		bookVO.setAuthor("wudi");
		bookVO.setPublish_date(new Date());
		bookVO.setGt_word_count(1000);
		bookVO.setLt_word_count(2000);
		bookVO.setId("4");
		bookVO.setTitle("elasticsearch学习");
		bookVO.setWord_count(5000);
		Serializable save = bookServiceTest.save(bookVO);
		assertEquals("success", save.toString());
	}
	
	/**
	 * 批量保存docs
	 */
	@Test
	public void bulkDocs(){
		List<BookVO> list = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			BookVO bookVO = new BookVO();
			bookVO.setAuthor("wudi");
			bookVO.setPublish_date(new Date());
			bookVO.setGt_word_count(1000);
			bookVO.setLt_word_count(2000);
			bookVO.setId(String.valueOf(i));
			bookVO.setTitle("elasticsearch学习");
			bookVO.setWord_count(5000);
			list.add(bookVO);
		}
		bookServiceTest.addList(list);
	}
	
	/**
	 * 根据id删除doc
	 */
	@Test
	public void deleteByIdTest(){
		String id = "7";
		bookServiceTest.deleteById(id);
	}
	
	/**
	 * 批量删除
	 */
	@Test
	public void deleteListTest(){
		List<BookVO> list = new ArrayList<>();
		for (int i = 5; i < 7; i++) {
			BookVO bookVO = new BookVO();
			bookVO.setAuthor("wudi");
			bookVO.setPublish_date(new Date());
			bookVO.setGt_word_count(1000);
			bookVO.setLt_word_count(2000);
			bookVO.setId(String.valueOf(i));
			bookVO.setTitle("elasticsearch学习");
			bookVO.setWord_count(5000);
			list.add(bookVO);
		}
		bookServiceTest.deleteList(list);
	}
	
	@Test
	public void queryStatisticsTest(){
		SearchField childField = new SearchField("author", FieldType.String, null);
		SearchField searchField = new SearchField("publish_date", FieldType.Date, "yyyy-MM-dd", 24*60*60*1000, childField);
		List<QueryCondition> conditions = new ArrayList<>();
		List<Map<String,Object>> list = bookServiceTest.queryStatistics(conditions, searchField);
	}
}
