package com.example.elasticsearch.vo;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.format.annotation.DateTimeFormat;

import com.example.elasticsearch.model.PrimaryKey;

/**
 * @author wudi
 * @version 创建时间：2018年7月22日 下午5:21:21
 * @ClassName BookVO
 * @Description book成员变量
 */
@PrimaryKey(value="id")
public class BookVO {

	private String id; //id索引
	private String title; // 标题
	private String author; // 作者名称
	private Integer word_count; // 字数
	@DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
	private Date publish_date; // 出版日期
	private Integer gt_word_count; //大于某个数
	private Integer lt_word_count; //小于某个数
	private PersonVO personVO;
	private Map<String,Object> mapVO;
	private List<String> list;
	
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public Integer getWord_count() {
		return word_count;
	}

	public void setWord_count(Integer word_count) {
		this.word_count = word_count;
	}

	public Date getPublish_date() {
		return publish_date;
	}

	public void setPublish_date(Date publish_date) {
		this.publish_date = publish_date;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getGt_word_count() {
		return gt_word_count;
	}

	public void setGt_word_count(Integer gt_word_count) {
		this.gt_word_count = gt_word_count;
	}

	public Integer getLt_word_count() {
		return lt_word_count;
	}

	public void setLt_word_count(Integer lt_word_count) {
		this.lt_word_count = lt_word_count;
	}

	public PersonVO getPersonVO() {
		return personVO;
	}

	public void setPersonVO(PersonVO personVO) {
		this.personVO = personVO;
	}

	public Map<String, Object> getMapVO() {
		return mapVO;
	}

	public void setMapVO(Map<String, Object> mapVO) {
		this.mapVO = mapVO;
	}

	public List<String> getList() {
		return list;
	}

	public void setList(List<String> list) {
		this.list = list;
	}

	
	
	
}
