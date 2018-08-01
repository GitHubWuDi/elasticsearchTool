package com.example.elasticsearch;

import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.elasticsearch.service.test.BookServiceTest;
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
	
	
   
}
