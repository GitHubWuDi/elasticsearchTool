package com.example.elasticsearch;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.elasticsearch.service.test.BookServiceTest;
import com.example.elasticsearch.util.page.PageReq;
import com.example.elasticsearch.util.page.PageRes;
import com.example.elasticsearch.util.page.QueryCondition;
import com.example.elasticsearch.vo.BookVO;

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
	 * 索引是否存在
	 */
	@Test
	public void isExistEsIndexTest() {
		Boolean result = bookServiceTest.isEsIndexExist();
		assertEquals(true, result);
	}

	/**
	 * 索引状态是开启还是关闭
	 */
	@Test
	public void checkESIndexStateTest(){
		String checkESIndexState = bookServiceTest.checkESIndexState();
		assertEquals("OPEN", checkESIndexState);
	}
	
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
	
	
}
