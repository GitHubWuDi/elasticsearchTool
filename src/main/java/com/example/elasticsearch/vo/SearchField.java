package com.example.elasticsearch.vo;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.springframework.util.StringUtils;

import com.example.elasticsearch.enums.FieldType;
import com.example.elasticsearch.exception.ElasticSearchErrorEnum;
import com.example.elasticsearch.exception.ElasticSearchException;
import com.example.elasticsearch.util.DateUtil;

import lombok.Data;

/**
 * @author wudi
 * @version 创建时间：2018年7月22日 下午5:21:21
 * @ClassName SearchField
 * @Description 分组查询
 */
@Data
public class SearchField {
	
	private static Logger logger  = Logger.getLogger(SearchField.class);
	
	private String fieldName;
	private FieldType fieldType;
	private String timeFormat;
	private long timeSpan = -1;
	private DateHistogramInterval timeInterval;
	private SearchField childField;
	private List<RangeVO> rangeList;
	private Integer from; //从开始个数
	private Integer size; //每次的增幅

	/**
	 * 增加一个分组字段
	 * 
	 * @param child
	 * @return
	 */
	public List<SearchField> getChildrenField() {
		List<SearchField> childrenField = new LinkedList<>();
		if (this.childField != null) {
			childrenField.add(this.childField);
		}
		return childrenField;
	}

	/**
	 * 构造函数一
	 * @param name
	 * @param type
	 * @param child
	 */
	public SearchField(String name, FieldType type, SearchField child) {
		checkChildFieldType(type, child);
		this.setFieldName(name);
		this.setFieldType(type);
		if (type == FieldType.Date) {
			if (timeSpan <= 0) {
				timeSpan = 24 * 3600 * 1000;// 一天
			}
			if (StringUtils.isEmpty(timeFormat)) {
				timeFormat = DateUtil.DEFAULT_DATE_PATTERN;
			}
		}
		this.setChildField(child);
	}
	
	public SearchField(String name, FieldType type, SearchField child,Integer from,Integer size) {
		checkChildFieldType(type, child);
		this.setFieldName(name);
		this.setFieldType(type);
		if (type == FieldType.Date) {
			if (timeSpan <= 0) {
				timeSpan = 24 * 3600 * 1000;// 一天
			}
			if (StringUtils.isEmpty(timeFormat)) {
				timeFormat = DateUtil.DEFAULT_DATE_PATTERN;
			}
		}
		this.from = from;
	    this.size = size;
		this.setChildField(child);
	}
	
	
    
	/**
	 * String,Object类型不能是count相关类型的子类型，否则抛出异常
	 * @param type
	 * @param child
	 */
	private  void checkChildFieldType(FieldType type, SearchField child){
		if(child!=null){
			if(type==FieldType.NumberAvg ||type==FieldType.NumberMax||type==FieldType.NumberMin||type==FieldType.NumberSum||type==FieldType.ObjectDistinctCount){
				if(child.getFieldType()==FieldType.Date || child.getFieldType()==FieldType.String||child.getFieldType()==FieldType.Object||child.getFieldType()==FieldType.Range){
					logger.error("String,Object类型Field不能是count相关类型的子类型,请检查:");
					throw new ElasticSearchException(ElasticSearchErrorEnum.INDEX_IS_EXIST);
				}
			}
		}
	}
	
	
	/**
	 * 构造函数-2
	 * @param name
	 * @param type
	 * @param format
	 * @param span
	 * @param child
	 */
	public SearchField(String name, FieldType type, String format, long span, SearchField child) {
		checkChildFieldType(type, child);
		this.setFieldName(name);
		this.setFieldType(type);
		this.setTimeFormat(format);
		this.setTimeSpan(span);
		this.setChildField(child);
	}
	
	
	
	/**
	 * 构造函数-三
	 * @param name
	 * @param type
	 * @param format
	 * @param span
	 * @param child
	 * @param from
	 * @param size
	 */
	public SearchField(String name, FieldType type, String format, long span, SearchField child,Integer from,Integer size) {
		checkChildFieldType(type, child);
		this.setFieldName(name);
		this.setFieldType(type);
		this.setTimeFormat(format);
		this.setTimeSpan(span);
		this.setFrom(from);
		this.setSize(size);
		this.setChildField(child);
	}
	

	/**
	 * 构造函数-4
	 * @param name
	 * @param type
	 * @param format
	 * @param timeInterval
	 * @param child
	 */
	public SearchField(String name, FieldType type, String format, DateHistogramInterval timeInterval,
			SearchField child) {
		checkChildFieldType(type, child);
		this.setFieldName(name);
		this.setFieldType(type);
		this.setTimeFormat(format);
		this.setTimeInterval(timeInterval);
		this.setChildField(child);
	}
	
	/**
	 * 构造函数-5
	 * @param name
	 * @param type
	 * @param format
	 * @param timeInterval
	 * @param child
	 * @param from
	 * @param size
	 */
	public SearchField(String name, FieldType type, String format, DateHistogramInterval timeInterval,
			SearchField child,Integer from,Integer size) {
		checkChildFieldType(type, child);
		this.setFieldName(name);
		this.setFieldType(type);
		this.setTimeFormat(format);
		this.setTimeInterval(timeInterval);
		this.from = from;
		this.size = size;
		this.setChildField(child);
	}
	
	
	/**
	 * range构造函数
	 * @param fieldName
	 * @param type
	 * @param rangeList
	 * @param child
	 */
	public SearchField(String fieldName,FieldType type,List<RangeVO> rangeList,SearchField child){
		checkChildFieldType(type, child);
		this.setFieldName(fieldName);
		this.setFieldType(type);
		this.setRangeList(rangeList);
		this.setChildField(child);
	}
	
	
	public SearchField(String fieldName,FieldType type,List<RangeVO> rangeList,SearchField child,Integer from,Integer size){
		checkChildFieldType(type, child);
		this.setFieldName(fieldName);
		this.setFieldType(type);
		this.setRangeList(rangeList);
		this.setChildField(child);
        this.from = from;
		this.size = size;
	}
	
	
	
}