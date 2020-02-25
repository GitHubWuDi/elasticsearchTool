package com.example.elasticsearch.vo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.example.elasticsearch.util.DateUtil;
import com.example.elasticsearch.util.page.PageRes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import lombok.Data;

/**
 * @author wudi
 * @version 创建时间：2018年7月22日 下午5:21:21
 * @ClassName ElasticSearchVO
 * @Description ES数据查询结构化
 */
@Data
public class ElasticSearchVO<T> {
     
	private String _scroll_id; //游标ID
	private int took;
	private boolean timed_out;
	private Shards _shards;
	private Hits<T> hits;
	
	@Data
	public class Shards{
	       private long total; //总数
	       private long successful; //成功数
	       private long skipped; //跳过数
	       private long failed;//失败数
	}
	
	@Data
	public static class Hits<T>{
	     private long total; //总数
	     private float max_score; //分数
	     private List<HitsChild<T>> hits; //总命中数
	}
	
	@Data
	public static class HitsChild<T>{
	      private String _index; //索引
	      private String _type; //类型
	      private String _id; //id
	      private float _score; //分数
	      private T _source; //对应资源   
	}
	
	/**
	 * 数据格式化
	 * @param clazz
	 * @return
	 */
	public List<T> getList(Class<T> clazz){
		List<T> list = new ArrayList<>();
		Gson gson = new GsonBuilder().setDateFormat(DateUtil.DEFAULT_DATE_PATTERN).create();
		List<HitsChild<T>> hits = getHits().getHits();
		for (HitsChild<T> hitsChild : hits) {
			T source = hitsChild.get_source();
			String json = gson.toJson(source);
			T t = gson.fromJson(json, clazz);
			list.add(t);
		}
		return list;
	}
	
	/**
	 * map对应结构的转换
	 * @return
	 */
	public List<Map<String,Object>> getList(){
		List<Map<String,Object>> list = new ArrayList<>();
		Gson gson = new GsonBuilder().setDateFormat(DateUtil.DEFAULT_DATE_PATTERN).create();
		List<HitsChild<T>> hits = getHits().getHits();
		for (HitsChild<T> hitsChild : hits) {
			T source = hitsChild.get_source();
			String json = gson.toJson(source);
			Map<String,Object> map= gson.fromJson(json,  new TypeToken<Map<String,Object>>(){}.getType());
			list.add(map);
		}
		return list;
	}
	
	/**
	 * 分页数据结构化
	 * @param clazz
	 * @return
	 */
	public PageRes<T> toPaginationResponse(Class<T> clazz){
		PageRes<T> findByPage = new PageRes<T>();
		List<T> listResult =getList(clazz);
		findByPage.setList(listResult);
		long total = getHits().getTotal();
		findByPage.setTotal(total);
		return findByPage;
	}
	
	public PageRes<Map<String,Object>> toPaginationResponse(){
		PageRes<Map<String,Object>> findByPage = new PageRes<Map<String,Object>>();
		List<Map<String,Object>> listResult =getList();
		findByPage.setList(listResult);
		long total = getHits().getTotal();
		findByPage.setTotal(total);
		return findByPage;
	}
}
