package com.example.elasticsearch.vo;

import java.util.ArrayList;
import java.util.List;

import com.example.elasticsearch.util.DateUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import lombok.Data;

@Data
public class ElasticSearchVO<T> {
     
	private int took;
	private boolean timed_out;
	private Shards _shards;
	private Hits<T> hits;
	
	@Data
	public class Shards{
	       private int total; //总数
	       private int successful; //成功数
	       private int skipped; //跳过数
	       private int failed;//失败数
	}
	
	@Data
	public static class Hits<T>{
	     private int total; //总数
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
	
	
}
